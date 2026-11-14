use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tracing::{debug, info, warn};

use crate::binance::{
    spawn_kline_ws_worker, spawn_liquidation_ws_worker, BinanceFuturesClient,
};
use crate::config::ScannerConfig;
use crate::indicators::{
    append_trade_plan, calculate_bollinger_bands, calculate_rsi, calculate_volume_ratio,
    detect_ema_crossover, CrossoverType,
};
use crate::models::{
    Candle, ScanSignal, SignalCategory, SignalDirection, Ticker24hr, WsForceOrderEvent,
    WsKlineEvent,
};
use crate::notifier::{format_compact_usd, format_price, AlertNotifier};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PersistedState {
    pub previous_oi: HashMap<String, f64>,
    pub cooldowns: HashMap<String, i64>,
}

pub struct FuturesScanner {
    config: ScannerConfig,
    client: BinanceFuturesClient,
    notifier: AlertNotifier,
    candles: Arc<RwLock<HashMap<String, Vec<Candle>>>>,
    tickers: Arc<RwLock<HashMap<String, Ticker24hr>>>,
    previous_oi: Arc<RwLock<HashMap<String, f64>>>,
}

impl FuturesScanner {
    pub fn new(config: ScannerConfig, notifier: AlertNotifier) -> Self {
        let client = BinanceFuturesClient::new(config.clone());
        Self {
            config,
            client,
            notifier,
            candles: Arc::new(RwLock::new(HashMap::new())),
            tickers: Arc::new(RwLock::new(HashMap::new())),
            previous_oi: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn load_state(&self) {
        let path = Path::new(&self.config.state_file);
        if !path.exists() {
            return;
        }
        if let Ok(content) = tokio::fs::read_to_string(path).await {
            if let Ok(state) = serde_json::from_str::<PersistedState>(&content) {
                let oi_len = state.previous_oi.len();
                let cd_len = state.cooldowns.len();
                {
                    let mut guard = self.previous_oi.write().await;
                    *guard = state.previous_oi;
                }
                self.notifier.load_cooldowns(state.cooldowns).await;
                info!(
                    "โหลดสถานะเดิมจาก {} สำเร็จ (OI {} เหรียญ, Cooldown {} รายการ)",
                    self.config.state_file, oi_len, cd_len
                );
            }
        }
    }

    pub async fn save_state(&self) {
        let previous_oi = self.previous_oi.read().await.clone();
        let cooldowns = self.notifier.export_cooldowns().await;
        let state = PersistedState {
            previous_oi,
            cooldowns,
        };
        if let Ok(json_str) = serde_json::to_string_pretty(&state) {
            if let Err(err) = tokio::fs::write(&self.config.state_file, json_str).await {
                warn!("ไม่สามารถบันทึกไฟล์สถานะ {}: {:#}", self.config.state_file, err);
            }
        }
    }

    /// Initializes the symbol universe, warms up historical klines, and evaluates an initial snapshot.
    /// Returns the list of qualified symbols and the number of signals detected during the initial scan.
    pub async fn initialize_and_scan_once(&self, emit_initial_signals: bool) -> Result<(Vec<String>, usize)> {
        self.load_state().await;

        info!(
            "กำลังดึงรายชื่อเหรียญ USDT-M Perpetual Futures ที่มี Volume 24h >= ${}...",
            format_compact_usd(self.config.min_quote_volume_24h)
        );

        let (symbols, ticker_map) = self.client.fetch_qualified_usdt_perpetuals().await?;
        info!(
            "พบเหรียญ USDT-M Perpetual ที่ผ่านเกณฑ์ทั้งหมด {} เหรียญ (สูงสุด {} เหรียญ)",
            symbols.len(),
            self.config.max_symbols
        );

        {
            let mut t_guard = self.tickers.write().await;
            *t_guard = ticker_map;
        }

        // Warm up historical klines concurrently (max 10 requests in flight to respect Binance rate limits)
        let semaphore = Arc::new(Semaphore::new(10));
        let mut join_handles = Vec::with_capacity(symbols.len());

        for sym in &symbols {
            let sem = semaphore.clone();
            let client = self.client.clone();
            let symbol = sym.clone();
            let interval = self.config.kline_interval.clone();
            let limit = self.config.candle_buffer_size;

            join_handles.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.ok()?;
                match client.fetch_klines(&symbol, &interval, limit).await {
                    Ok(klines) => Some((symbol, klines)),
                    Err(err) => {
                        warn!("ดึงข้อมูลแท่งเทียนเริ่มต้นของ {} ไม่สำเร็จ: {:#}", symbol, err);
                        None
                    }
                }
            }));
        }

        let mut loaded_count = 0usize;
        let mut signal_count = 0usize;

        for handle in join_handles {
            if let Ok(Some((symbol, klines))) = handle.await {
                loaded_count += 1;
                if emit_initial_signals {
                    let signals = self.evaluate_symbol_candles(&symbol, &klines, true).await;
                    signal_count += signals.len();
                    for sig in signals {
                        self.notifier.dispatch(sig).await;
                    }
                }
                let mut c_guard = self.candles.write().await;
                c_guard.insert(symbol, klines);
            }
        }

        info!(
            "โหลดข้อมูลแท่งเทียนย้อนหลัง ({}) สำเร็จ {}/{} เหรียญ",
            self.config.kline_interval,
            loaded_count,
            symbols.len()
        );

        // Scan Funding Rates & initialize Open Interest baseline
        let active_set: HashSet<String> = symbols.iter().cloned().collect();
        let fr_signals = self.scan_funding_rates(&active_set).await;
        signal_count += fr_signals;

        let oi_signals = self.scan_open_interest(&symbols).await;
        signal_count += oi_signals;

        self.save_state().await;

        Ok((symbols, signal_count))
    }

    /// Runs the full real-time scanner loop (WebSocket Kline Streams + Liquidation Stream + Futures Metric Poller).
    pub async fn run_forever(self: Arc<Self>) -> Result<()> {
        let (symbols, initial_signals) = self.initialize_and_scan_once(true).await?;
        info!(
            "เริ่มต้นสแกนเรียลไทม์สำเร็จ! (พบสัญญาณรอบแรก {} รายการ)",
            initial_signals
        );

        let (kline_tx, mut kline_rx) = mpsc::channel::<WsKlineEvent>(2048);
        let (liq_tx, mut liq_rx) = mpsc::channel::<WsForceOrderEvent>(1024);

        // Split symbols into chunks of 100 per WebSocket connection for stability
        for chunk in symbols.chunks(100) {
            spawn_kline_ws_worker(
                self.config.ws_base_url.clone(),
                chunk.to_vec(),
                self.config.kline_interval.clone(),
                kline_tx.clone(),
            );
        }

        // Spawn All-Market Liquidation stream (!forceOrder@arr)
        spawn_liquidation_ws_worker(self.config.ws_base_url.clone(), liq_tx);

        // Spawn periodic poller for Funding Rates, Open Interest Spikes, and 24hr Ticker refresh
        let scanner_poller = self.clone();
        let tracked_symbols = symbols.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                scanner_poller.config.futures_poll_interval_secs,
            ));
            // Skip first immediate tick since initialize_and_scan_once already ran
            interval.tick().await;

            let active_set: HashSet<String> = tracked_symbols.iter().cloned().collect();

            loop {
                interval.tick().await;
                debug!("กำลังอัปเดตข้อมูล Funding Rate, Open Interest และ 24h Ticker...");

                if let Ok((_, new_tickers)) =
                    scanner_poller.client.fetch_qualified_usdt_perpetuals().await
                {
                    let mut t_guard = scanner_poller.tickers.write().await;
                    for (k, v) in new_tickers {
                        t_guard.insert(k, v);
                    }
                }

                scanner_poller.scan_funding_rates(&active_set).await;
                scanner_poller.scan_open_interest(&tracked_symbols).await;
                scanner_poller.save_state().await;
            }
        });

        if let Some(mins) = self.config.run_duration_mins {
            info!(
                "🟢 ระบบ Scanner-f กำลังทำงานแบบเรียลไทม์ (ตั้งเวลาทำงานสูงสุด {} นาที)...",
                mins
            );
            let sleep_fut = tokio::time::sleep(Duration::from_secs(mins * 60));
            tokio::pin!(sleep_fut);

            loop {
                tokio::select! {
                    _ = &mut sleep_fut => {
                        info!("ครบกำหนดเวลาทำงาน {} นาที กำลังบันทึกสถานะและปิดระบบอย่างปลอดภัย...", mins);
                        self.save_state().await;
                        break;
                    }
                    Some(kline_ev) = kline_rx.recv() => {
                        self.handle_kline_event(kline_ev).await;
                    }
                    Some(liq_ev) = liq_rx.recv() => {
                        self.handle_liquidation_event(liq_ev).await;
                    }
                    else => break,
                }
            }
        } else {
            info!("🟢 ระบบ Scanner-f กำลังทำงานแบบเรียลไทม์ (กด Ctrl+C เพื่อหยุดการทำงาน)...");
            loop {
                tokio::select! {
                    Some(kline_ev) = kline_rx.recv() => {
                        self.handle_kline_event(kline_ev).await;
                    }
                    Some(liq_ev) = liq_rx.recv() => {
                        self.handle_liquidation_event(liq_ev).await;
                    }
                    else => break,
                }
            }
        }

        Ok(())
    }

    async fn handle_kline_event(&self, ev: WsKlineEvent) {
        let new_candle = ev.kline.to_candle();
        let symbol = ev.symbol;

        let snapshot = {
            let mut guard = self.candles.write().await;
            let buf = guard.entry(symbol.clone()).or_insert_with(Vec::new);

            if let Some(last) = buf.last_mut() {
                if last.open_time == new_candle.open_time {
                    *last = new_candle;
                } else if new_candle.open_time > last.open_time {
                    last.is_closed = true;
                    buf.push(new_candle);
                    if buf.len() > self.config.candle_buffer_size {
                        let excess = buf.len() - self.config.candle_buffer_size;
                        buf.drain(0..excess);
                    }
                }
            } else {
                buf.push(new_candle);
            }

            buf.clone()
        };

        let signals = self
            .evaluate_symbol_candles(&symbol, &snapshot, new_candle.is_closed)
            .await;

        for sig in signals {
            self.notifier.dispatch(sig).await;
        }
    }

    async fn handle_liquidation_event(&self, ev: WsForceOrderEvent) {
        let order = ev.order;
        if !order.symbol.ends_with("USDT") {
            return;
        }

        let usd_val = order.usd_value();
        if usd_val < self.config.min_liquidation_usd {
            return;
        }

        let (change_24h, vol_24h) = self.get_ticker_meta(&order.symbol).await;

        // In Binance forceOrder: Side "SELL" means a Long position was force-sold (Long Liquidated -> Bearish pressure)
        // Side "BUY" means a Short position was force-bought (Short Liquidated -> Bullish short squeeze)
        let (liq_side_label, direction) = if order.side == "SELL" {
            ("Long ถูกบังคับปิด (Long Liquidated 🩸)", SignalDirection::Bearish)
        } else {
            ("Short ถูกบังคับปิด (Short Liquidated 🚀)", SignalDirection::Bullish)
        };

        let order_price = order.price_f64();
        let order_qty = order.qty_f64();

        let signal = ScanSignal {
            symbol: order.symbol,
            category: SignalCategory::LargeLiquidation,
            direction,
            price: order_price,
            change_24h_pct: change_24h,
            quote_volume_24h: vol_24h,
            timeframe: "Real-time".to_string(),
            metric_summary: format!(
                "{} มูลค่า ${}",
                liq_side_label,
                format_compact_usd(usd_val)
            ),
            details: vec![
                ("มูลค่า Liquidation".to_string(), format!("${}", format_compact_usd(usd_val))),
                ("จำนวน (Qty)".to_string(), format!("{:.4}", order_qty)),
                ("ราคาเฉลี่ย".to_string(), format!("${}", format_price(order_price))),
            ],
            timestamp: Utc::now(),
        };

        self.notifier.dispatch(signal).await;
    }

    /// Evaluates price action, volume spikes, and technical indicators on a symbol's candle buffer.
    async fn evaluate_symbol_candles(
        &self,
        symbol: &str,
        candles: &[Candle],
        check_closed_indicators: bool,
    ) -> Vec<ScanSignal> {
        let mut signals = Vec::new();
        if candles.len() < 5 {
            return signals;
        }

        let latest = match candles.last() {
            Some(c) => *c,
            None => return signals,
        };

        let (change_24h, vol_24h) = self.get_ticker_meta(symbol).await;
        let tf = self.config.kline_interval.clone();

        // 1. Real-time Price Pump / Dump check on current candle
        let candle_chg = latest.change_pct();
        let vol_ratio_opt = calculate_volume_ratio(candles, self.config.volume_lookback);

        if candle_chg >= self.config.price_change_threshold_pct {
            let mut details = vec![
                ("การเปลี่ยนแปลงในแท่ง".to_string(), format!("+{:.2}%", candle_chg)),
                ("Open -> Price".to_string(), format!("${} -> ${}", format_price(latest.open), format_price(latest.close))),
            ];
            if let Some((ratio, _)) = vol_ratio_opt {
                details.push(("Relative Volume".to_string(), format!("{:.2}x ของค่าเฉลี่ย", ratio)));
            }
            signals.push(ScanSignal {
                symbol: symbol.to_string(),
                category: SignalCategory::PricePump,
                direction: SignalDirection::Bullish,
                price: latest.close,
                change_24h_pct: change_24h,
                quote_volume_24h: vol_24h,
                timeframe: tf.clone(),
                metric_summary: format!("ราคาพุ่ง +{:.2}% ในแท่ง {}", candle_chg, tf),
                details,
                timestamp: Utc::now(),
            });
        } else if candle_chg <= -self.config.price_change_threshold_pct {
            let mut details = vec![
                ("การเปลี่ยนแปลงในแท่ง".to_string(), format!("{:.2}%", candle_chg)),
                ("Open -> Price".to_string(), format!("${} -> ${}", format_price(latest.open), format_price(latest.close))),
            ];
            if let Some((ratio, _)) = vol_ratio_opt {
                details.push(("Relative Volume".to_string(), format!("{:.2}x ของค่าเฉลี่ย", ratio)));
            }
            signals.push(ScanSignal {
                symbol: symbol.to_string(),
                category: SignalCategory::PriceDump,
                direction: SignalDirection::Bearish,
                price: latest.close,
                change_24h_pct: change_24h,
                quote_volume_24h: vol_24h,
                timeframe: tf.clone(),
                metric_summary: format!("ราคาทุบ {:.2}% ในแท่ง {}", candle_chg, tf),
                details,
                timestamp: Utc::now(),
            });
        }

        // 2. Real-time Volume Spike check
        if let Some((vol_ratio, avg_vol)) = vol_ratio_opt {
            if vol_ratio >= self.config.volume_spike_multiplier {
                let direction = if candle_chg >= 0.0 {
                    SignalDirection::Bullish
                } else {
                    SignalDirection::Bearish
                };
                signals.push(ScanSignal {
                    symbol: symbol.to_string(),
                    category: SignalCategory::VolumeSpike,
                    direction,
                    price: latest.close,
                    change_24h_pct: change_24h,
                    quote_volume_24h: vol_24h,
                    timeframe: tf.clone(),
                    metric_summary: format!(
                        "วอลุ่มพุ่ง {:.2} เท่าของค่าเฉลี่ย {} แท่ง ({:+.2}%)",
                        vol_ratio, self.config.volume_lookback, candle_chg
                    ),
                    details: vec![
                        ("Volume แท่งปัจจุบัน".to_string(), format!("${}", format_compact_usd(latest.quote_volume))),
                        ("Volume เฉลี่ย".to_string(), format!("${}", format_compact_usd(avg_vol))),
                        ("การเปลี่ยนแปลงราคา".to_string(), format!("{:+.2}%", candle_chg)),
                    ],
                    timestamp: Utc::now(),
                });
            }
        }

        // 3. Technical Indicators (evaluated on closed candles to prevent repainting)
        if !check_closed_indicators {
            return signals;
        }

        // Use completed candles for indicator evaluation
        let eval_slice = if candles.last().map(|c| !c.is_closed).unwrap_or(false) && candles.len() > 1 {
            &candles[..candles.len() - 1]
        } else {
            candles
        };

        let closes: Vec<f64> = eval_slice.iter().map(|c| c.close).collect();
        let eval_price = *closes.last().unwrap_or(&latest.close);

        // 3a. RSI Overbought / Oversold
        if let Some(rsi) = calculate_rsi(&closes, self.config.rsi_period) {
            if rsi >= self.config.rsi_overbought {
                signals.push(ScanSignal {
                    symbol: symbol.to_string(),
                    category: SignalCategory::RsiOverbought,
                    direction: SignalDirection::Bearish,
                    price: eval_price,
                    change_24h_pct: change_24h,
                    quote_volume_24h: vol_24h,
                    timeframe: tf.clone(),
                    metric_summary: format!(
                        "RSI({}) = {:.2} (Overbought >= {:.0})",
                        self.config.rsi_period, rsi, self.config.rsi_overbought
                    ),
                    details: vec![
                        (format!("RSI({})", self.config.rsi_period), format!("{:.2}", rsi)),
                        ("สถานะ".to_string(), "Overbought (ระวังแรงเทขาย/ย่อตัว)".to_string()),
                    ],
                    timestamp: Utc::now(),
                });
            } else if rsi <= self.config.rsi_oversold {
                signals.push(ScanSignal {
                    symbol: symbol.to_string(),
                    category: SignalCategory::RsiOversold,
                    direction: SignalDirection::Bullish,
                    price: eval_price,
                    change_24h_pct: change_24h,
                    quote_volume_24h: vol_24h,
                    timeframe: tf.clone(),
                    metric_summary: format!(
                        "RSI({}) = {:.2} (Oversold <= {:.0})",
                        self.config.rsi_period, rsi, self.config.rsi_oversold
                    ),
                    details: vec![
                        (format!("RSI({})", self.config.rsi_period), format!("{:.2}", rsi)),
                        ("สถานะ".to_string(), "Oversold (ลุ้นรีบาวด์/กลับตัวขึ้น)".to_string()),
                    ],
                    timestamp: Utc::now(),
                });
            }
        }

        // 3b. EMA Fast / Slow Crossover
        if let Some(ema_res) = detect_ema_crossover(
            &closes,
            self.config.ema_fast_period,
            self.config.ema_slow_period,
        ) {
            match ema_res.crossover {
                Some(CrossoverType::Bullish) => {
                    signals.push(ScanSignal {
                        symbol: symbol.to_string(),
                        category: SignalCategory::EmaBullishCross,
                        direction: SignalDirection::Bullish,
                        price: eval_price,
                        change_24h_pct: change_24h,
                        quote_volume_24h: vol_24h,
                        timeframe: tf.clone(),
                        metric_summary: format!(
                            "EMA({}) ตัดขึ้นเหนือ EMA({}) (Golden Cross)",
                            self.config.ema_fast_period, self.config.ema_slow_period
                        ),
                        details: vec![
                            (format!("EMA({})", self.config.ema_fast_period), format_price(ema_res.fast_now)),
                            (format!("EMA({})", self.config.ema_slow_period), format_price(ema_res.slow_now)),
                        ],
                        timestamp: Utc::now(),
                    });
                }
                Some(CrossoverType::Bearish) => {
                    signals.push(ScanSignal {
                        symbol: symbol.to_string(),
                        category: SignalCategory::EmaBearishCross,
                        direction: SignalDirection::Bearish,
                        price: eval_price,
                        change_24h_pct: change_24h,
                        quote_volume_24h: vol_24h,
                        timeframe: tf.clone(),
                        metric_summary: format!(
                            "EMA({}) ตัดลงต่ำกว่า EMA({}) (Death Cross)",
                            self.config.ema_fast_period, self.config.ema_slow_period
                        ),
                        details: vec![
                            (format!("EMA({})", self.config.ema_fast_period), format_price(ema_res.fast_now)),
                            (format!("EMA({})", self.config.ema_slow_period), format_price(ema_res.slow_now)),
                        ],
                        timestamp: Utc::now(),
                    });
                }
                None => {}
            }
        }

        // 3c. Bollinger Bands Breakout accompanied by Volume
        if let Some(bb) =
            calculate_bollinger_bands(&closes, self.config.bb_period, self.config.bb_std_dev)
        {
            let eval_vol_ratio = calculate_volume_ratio(eval_slice, self.config.volume_lookback)
                .map(|(r, _)| r)
                .unwrap_or(1.0);

            if eval_price > bb.upper && eval_vol_ratio >= self.config.bb_min_volume_ratio {
                signals.push(ScanSignal {
                    symbol: symbol.to_string(),
                    category: SignalCategory::BollingerUpperBreakout,
                    direction: SignalDirection::Bullish,
                    price: eval_price,
                    change_24h_pct: change_24h,
                    quote_volume_24h: vol_24h,
                    timeframe: tf.clone(),
                    metric_summary: format!(
                        "ราคาปิดทะลุ Upper Bollinger Band พร้อมวอลุ่ม {:.2}x",
                        eval_vol_ratio
                    ),
                    details: vec![
                        ("Upper Band".to_string(), format!("${}", format_price(bb.upper))),
                        ("Middle Band".to_string(), format!("${}", format_price(bb.middle))),
                        ("Bandwidth".to_string(), format!("{:.2}%", bb.bandwidth_pct)),
                        ("Relative Volume".to_string(), format!("{:.2}x", eval_vol_ratio)),
                    ],
                    timestamp: Utc::now(),
                });
            } else if eval_price < bb.lower && eval_vol_ratio >= self.config.bb_min_volume_ratio {
                signals.push(ScanSignal {
                    symbol: symbol.to_string(),
                    category: SignalCategory::BollingerLowerBreakout,
                    direction: SignalDirection::Bearish,
                    price: eval_price,
                    change_24h_pct: change_24h,
                    quote_volume_24h: vol_24h,
                    timeframe: tf,
                    metric_summary: format!(
                        "ราคาปิดหลุด Lower Bollinger Band พร้อมวอลุ่ม {:.2}x",
                        eval_vol_ratio
                    ),
                    details: vec![
                        ("Lower Band".to_string(), format!("${}", format_price(bb.lower))),
                        ("Middle Band".to_string(), format!("${}", format_price(bb.middle))),
                        ("Bandwidth".to_string(), format!("{:.2}%", bb.bandwidth_pct)),
                        ("Relative Volume".to_string(), format!("{:.2}x", eval_vol_ratio)),
                    ],
                    timestamp: Utc::now(),
                });
            }
        }

        for sig in &mut signals {
            append_trade_plan(&mut sig.details, sig.direction, sig.price, Some(candles));
        }

        signals
    }

    /// Scans all active symbols for extreme Funding Rates (`/fapi/v1/premiumIndex`).
    async fn scan_funding_rates(&self, active_symbols: &HashSet<String>) -> usize {
        let list = match self.client.fetch_all_premium_index().await {
            Ok(l) => l,
            Err(err) => {
                warn!("ไม่สามารถดึงข้อมูล Funding Rate ได้: {:#}", err);
                return 0;
            }
        };

        let mut count = 0usize;
        for item in list {
            if !active_symbols.contains(&item.symbol) {
                continue;
            }

            let fr_pct = item.funding_rate_pct();
            if fr_pct.abs() < self.config.funding_rate_threshold_pct {
                continue;
            }

            let (change_24h, vol_24h) = self.get_ticker_meta(&item.symbol).await;
            let direction = if fr_pct < 0.0 {
                // Strongly negative funding rate -> Shorts pay Longs -> Short Squeeze potential
                SignalDirection::Bullish
            } else {
                // Strongly positive funding rate -> Longs pay Shorts -> Crowded Longs
                SignalDirection::Bearish
            };

            let note = if fr_pct < 0.0 {
                "Funding ติดลบหนัก (ฝั่ง Short จ่าย Long — ระวัง Short Squeeze)"
            } else {
                "Funding เป็นบวกสูง (ฝั่ง Long จ่าย Short — Long หนาแน่น)"
            };

            let mark_price = item.mark_price_f64();
            let mut details = vec![
                ("Funding Rate".to_string(), format!("{:+.4}%", fr_pct)),
                ("Mark Price".to_string(), format!("${}", format_price(mark_price))),
            ];
            append_trade_plan(&mut details, direction, mark_price, None);

            let sig = ScanSignal {
                symbol: item.symbol,
                category: SignalCategory::FundingRateExtreme,
                direction,
                price: mark_price,
                change_24h_pct: change_24h,
                quote_volume_24h: vol_24h,
                timeframe: "Funding".to_string(),
                metric_summary: format!("Funding Rate = {:+.4}% ({})", fr_pct, note),
                details,
                timestamp: Utc::now(),
            };

            count += 1;
            self.notifier.dispatch(sig).await;
        }

        count
    }

    /// Polls Open Interest across symbols and detects sudden spikes (`oi_spike_threshold_pct`).
    async fn scan_open_interest(&self, symbols: &[String]) -> usize {
        let semaphore = Arc::new(Semaphore::new(8));
        let mut handles = Vec::with_capacity(symbols.len());

        for sym in symbols {
            let sem = semaphore.clone();
            let client = self.client.clone();
            let symbol = sym.clone();
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.ok()?;
                let oi_resp = client.fetch_open_interest(&symbol).await.ok()?;
                Some((symbol, oi_resp.open_interest_f64()))
            }));
        }

        let mut count = 0usize;
        for handle in handles {
            if let Ok(Some((symbol, current_oi))) = handle.await {
                if current_oi <= 0.0 {
                    continue;
                }

                let prev_opt = {
                    let mut guard = self.previous_oi.write().await;
                    let prev = guard.get(&symbol).copied();
                    guard.insert(symbol.clone(), current_oi);
                    prev
                };

                if let Some(prev_oi) = prev_opt {
                    if prev_oi > 0.0 {
                        let oi_change_pct = ((current_oi - prev_oi) / prev_oi) * 100.0;
                        if oi_change_pct.abs() >= self.config.oi_spike_threshold_pct {
                            let (change_24h, vol_24h) = self.get_ticker_meta(&symbol).await;
                            let price = self.get_latest_price(&symbol).await;
                            let direction = if oi_change_pct > 0.0 {
                                SignalDirection::Bullish
                            } else {
                                SignalDirection::Bearish
                            };

                            let oi_usd = current_oi * price;
                            let sig = ScanSignal {
                                symbol,
                                category: SignalCategory::OpenInterestSpike,
                                direction,
                                price,
                                change_24h_pct: change_24h,
                                quote_volume_24h: vol_24h,
                                timeframe: format!("{}s Poll", self.config.futures_poll_interval_secs),
                                metric_summary: format!(
                                    "Open Interest เปลี่ยนแปลง {:+.2}% (มูลค่ารวม ~${})",
                                    oi_change_pct,
                                    format_compact_usd(oi_usd)
                                ),
                                details: vec![
                                    ("OI Change".to_string(), format!("{:+.2}%", oi_change_pct)),
                                    ("OI ปัจจุบัน (Contracts)".to_string(), format_compact_usd(current_oi)),
                                    ("มูลค่า OI (USDT)".to_string(), format!("${}", format_compact_usd(oi_usd))),
                                ],
                                timestamp: Utc::now(),
                            };

                            count += 1;
                            self.notifier.dispatch(sig).await;
                        }
                    }
                }
            }
        }

        count
    }

    async fn get_ticker_meta(&self, symbol: &str) -> (Option<f64>, Option<f64>) {
        let guard = self.tickers.read().await;
        if let Some(t) = guard.get(symbol) {
            (
                Some(t.price_change_pct_f64()),
                Some(t.quote_volume_f64()),
            )
        } else {
            (None, None)
        }
    }

    async fn get_latest_price(&self, symbol: &str) -> f64 {
        {
            let c_guard = self.candles.read().await;
            if let Some(list) = c_guard.get(symbol) {
                if let Some(last) = list.last() {
                    return last.close;
                }
            }
        }
        let t_guard = self.tickers.read().await;
        t_guard
            .get(symbol)
            .map(|t| t.last_price_f64())
            .unwrap_or(0.0)
    }
}
