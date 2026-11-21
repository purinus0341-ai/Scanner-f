use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Notify, RwLock, Semaphore};
use tracing::{debug, info, warn};

use crate::binance::{
    spawn_kline_ws_worker, spawn_liquidation_ws_worker, BinanceFuturesClient,
};
use crate::config::ScannerConfig;
use crate::indicators::{
    analyze_timeframe_state, append_mtf_trade_plan, append_trade_plan, calculate_bollinger_bands,
    calculate_rsi, calculate_volume_ratio, detect_ema_crossover, evaluate_mtf_confluence,
    CrossoverType,
};
use crate::models::{
    Candle, ScanSignal, SignalCategory, SignalDirection, Ticker24hr, WsForceOrderEvent,
    WsKlineEvent,
};
use crate::notifier::{format_compact_usd, format_price, AlertNotifier};
use crate::paper_trader::PaperTrader;
use crate::telegram_bot::TelegramBotController;

const CACHE_1H_TTL_SECS: i64 = 300;
const MID_CANDLE_MTF_THROTTLE_SECS: i64 = 30;
const VOLUME_SPIKE_MIN_MOVE_PCT: f64 = 0.6;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PersistedState {
    pub previous_oi: HashMap<String, f64>,
    pub cooldowns: HashMap<String, i64>,
}

#[derive(Debug, Default)]
struct ScanMetrics {
    ws_5m_ticks: AtomicU64,
    ws_15m_ticks: AtomicU64,
    closed_5m_bars: AtomicU64,
    closed_15m_bars: AtomicU64,
    raw_triggers: AtomicU64,
    passed_signals: AtomicU64,
}

fn upsert_candle(buf: &mut Vec<Candle>, new_candle: Candle, max_len: usize) {
    if let Some(last) = buf.last_mut() {
        if last.open_time == new_candle.open_time {
            *last = new_candle;
        } else if new_candle.open_time > last.open_time {
            last.is_closed = true;
            buf.push(new_candle);
            if max_len > 0 && buf.len() > max_len {
                let excess = buf.len() - max_len;
                buf.drain(0..excess);
            }
        }
    } else {
        buf.push(new_candle);
    }
}

type TimedCandleMap = Arc<RwLock<HashMap<String, (i64, Vec<Candle>)>>>;

pub struct FuturesScanner {
    config: ScannerConfig,
    client: BinanceFuturesClient,
    notifier: AlertNotifier,
    paper_trader: PaperTrader,
    scan_now_notify: Arc<Notify>,
    candles: Arc<RwLock<HashMap<String, Vec<Candle>>>>,
    candles_15m: Arc<RwLock<HashMap<String, Vec<Candle>>>>,
    candles_1h_cache: TimedCandleMap,
    mid_candle_throttle: Arc<RwLock<HashMap<String, i64>>>,
    tickers: Arc<RwLock<HashMap<String, Ticker24hr>>>,
    previous_oi: Arc<RwLock<HashMap<String, f64>>>,
    metrics: Arc<ScanMetrics>,
}

impl FuturesScanner {
    pub fn new(config: ScannerConfig, notifier: AlertNotifier, paper_trader: PaperTrader) -> Self {
        let client = BinanceFuturesClient::new(config.clone());
        Self {
            config,
            client,
            notifier,
            paper_trader,
            scan_now_notify: Arc::new(Notify::new()),
            candles: Arc::new(RwLock::new(HashMap::new())),
            candles_15m: Arc::new(RwLock::new(HashMap::new())),
            candles_1h_cache: Arc::new(RwLock::new(HashMap::new())),
            mid_candle_throttle: Arc::new(RwLock::new(HashMap::new())),
            tickers: Arc::new(RwLock::new(HashMap::new())),
            previous_oi: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(ScanMetrics::default()),
        }
    }

    pub fn telegram_bot_controller(&self) -> Arc<TelegramBotController> {
        Arc::new(TelegramBotController::new(
            self.config.clone(),
            self.client.clone(),
            self.notifier.clone(),
            self.paper_trader.clone(),
            self.scan_now_notify.clone(),
        ))
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

    async fn log_scan_heartbeat(&self) {
        let t5 = self.metrics.ws_5m_ticks.swap(0, Ordering::Relaxed);
        let t15 = self.metrics.ws_15m_ticks.swap(0, Ordering::Relaxed);
        let c5 = self.metrics.closed_5m_bars.swap(0, Ordering::Relaxed);
        let c15 = self.metrics.closed_15m_bars.swap(0, Ordering::Relaxed);
        let raw = self.metrics.raw_triggers.swap(0, Ordering::Relaxed);
        let passed = self.metrics.passed_signals.swap(0, Ordering::Relaxed);

        if t5 == 0 && t15 == 0 && c5 == 0 && c15 == 0 && raw == 0 && passed == 0 {
            return;
        }

        let (active_strategy, eff_stars) =
            self.paper_trader.get_effective_strategy_and_min_stars().await;
        let lock_tag = if active_strategy.requires_1h_alignment() {
            "+1h"
        } else {
            ""
        };
        let ws_summary = if self.config.kline_interval == "15m" {
            format!("WS 15m={} ticks (ปิดแท่ง 15m={})", t15, c15)
        } else {
            format!(
                "WS {}={} / 15m={} ticks (ปิดแท่ง {}={}, 15m={})",
                self.config.kline_interval, t5, t15, self.config.kline_interval, c5, c15
            )
        };
        info!(
            "📡 สถานะสแกนล่าสุด [{} | ≥{}⭐{}]: {} | Trigger ตั้งต้น={} -> ผ่านเกณฑ์={}",
            active_strategy.short_tag(),
            eff_stars,
            lock_tag,
            ws_summary,
            raw,
            passed
        );
    }

    async fn warmup_interval_buffers(&self, symbols: &[String], interval: &str) -> usize {
        let semaphore = Arc::new(Semaphore::new(10));
        let mut join_handles = Vec::with_capacity(symbols.len());

        for sym in symbols {
            let sem = semaphore.clone();
            let client = self.client.clone();
            let symbol = sym.clone();
            let tf = interval.to_string();
            let limit = self.config.candle_buffer_size;

            join_handles.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.ok()?;
                match client.fetch_klines(&symbol, &tf, limit).await {
                    Ok(klines) => Some((symbol, klines)),
                    Err(err) => {
                        warn!("ดึงข้อมูลแท่งเทียนเริ่มต้น ({}) ของ {} ไม่สำเร็จ: {:#}", tf, symbol, err);
                        None
                    }
                }
            }));
        }

        let mut loaded_count = 0usize;
        let mut loaded_map = HashMap::with_capacity(symbols.len());
        for handle in join_handles {
            if let Ok(Some((symbol, klines))) = handle.await {
                loaded_count += 1;
                loaded_map.insert(symbol, klines);
            }
        }

        if interval == "15m" {
            let mut guard = self.candles_15m.write().await;
            for (k, v) in loaded_map {
                guard.insert(k, v);
            }
        } else {
            let mut guard = self.candles.write().await;
            for (k, v) in loaded_map {
                guard.insert(k, v);
            }
        }

        loaded_count
    }

    async fn get_or_fetch_candles_5m(&self, symbol: &str) -> Result<Vec<Candle>> {
        if self.config.kline_interval == "5m" {
            let guard = self.candles.read().await;
            if let Some(buf) = guard.get(symbol) {
                if buf.len() >= 25 {
                    return Ok(buf.clone());
                }
            }
        }
        let fetched = self.client.fetch_klines(symbol, "5m", 65).await?;
        if self.config.kline_interval == "5m" && !fetched.is_empty() {
            let mut guard = self.candles.write().await;
            guard.insert(symbol.to_string(), fetched.clone());
        }
        Ok(fetched)
    }

    async fn get_or_fetch_candles_15m(&self, symbol: &str) -> Result<Vec<Candle>> {
        {
            let guard = self.candles_15m.read().await;
            if let Some(buf) = guard.get(symbol) {
                if buf.len() >= 25 {
                    return Ok(buf.clone());
                }
            }
        }
        let fetched = self.client.fetch_klines(symbol, "15m", 65).await?;
        if !fetched.is_empty() {
            let mut guard = self.candles_15m.write().await;
            guard.insert(symbol.to_string(), fetched.clone());
        }
        Ok(fetched)
    }

    async fn fetch_klines_1h_cached(&self, symbol: &str) -> Result<Vec<Candle>> {
        let now = Utc::now().timestamp();
        {
            let guard = self.candles_1h_cache.read().await;
            if let Some((fetched_at, buf)) = guard.get(symbol) {
                if now - *fetched_at < CACHE_1H_TTL_SECS && !buf.is_empty() {
                    return Ok(buf.clone());
                }
            }
        }
        let fetched = self.client.fetch_klines(symbol, "1h", 65).await?;
        if !fetched.is_empty() {
            let mut guard = self.candles_1h_cache.write().await;
            guard.insert(symbol.to_string(), (now, fetched.clone()));
        }
        Ok(fetched)
    }

    async fn should_allow_mid_candle_eval(
        &self,
        symbol: &str,
        trigger_tf: &str,
        category: SignalCategory,
    ) -> bool {
        let now = Utc::now().timestamp();
        let key = format!("{}:{}:{}", symbol, trigger_tf, category.as_key());
        let mut guard = self.mid_candle_throttle.write().await;
        if let Some(&last_ts) = guard.get(&key) {
            if now - last_ts < MID_CANDLE_MTF_THROTTLE_SECS {
                return false;
            }
        }
        if guard.len() > 2048 {
            guard.retain(|_, &mut ts| now - ts < MID_CANDLE_MTF_THROTTLE_SECS * 4);
        }
        guard.insert(key, now);
        true
    }

    /// Re-evaluates in-memory 5m and 15m candle buffers against current MTF states (mirrors Python's 60s MTF sweep without extra 5m/15m REST calls).
    pub async fn rescan_cached_mtf_buffers(&self, symbols: &[String]) -> usize {
        let scan_15m_extra = self.config.kline_interval != "15m";
        let snapshot_primary = self.candles.read().await.clone();
        let snapshot_15m = if scan_15m_extra {
            Some(self.candles_15m.read().await.clone())
        } else {
            None
        };

        let mut signal_count = 0usize;
        for sym in symbols {
            if let Some(klines) = snapshot_primary.get(sym) {
                let signals = self
                    .evaluate_symbol_candles_for_tf(
                        sym,
                        &self.config.kline_interval,
                        klines,
                        true,
                    )
                    .await;
                signal_count += signals.len();
                for sig in signals {
                    self.notifier.dispatch(sig).await;
                }
            }
            if let Some(ref map_15m) = snapshot_15m {
                if let Some(klines_15m) = map_15m.get(sym) {
                    let signals_15m = self
                        .evaluate_symbol_candles_for_tf(sym, "15m", klines_15m, true)
                        .await;
                    signal_count += signals_15m.len();
                    for sig in signals_15m {
                        self.notifier.dispatch(sig).await;
                    }
                }
            }
        }
        signal_count
    }

    /// Initializes the symbol universe, warms up historical klines (primary TF + 15m), and evaluates an initial snapshot.
    /// Returns the list of qualified symbols and the number of signals detected during the initial scan.
    pub async fn initialize_and_scan_once(&self, emit_initial_signals: bool) -> Result<(Vec<String>, usize)> {
        self.load_state().await;

        info!(
            "กำลังดึงรายชื่อเหรียญ USDT-M Perpetual Futures ที่มี Volume 24h >= ${}...",
            format_compact_usd(self.config.min_quote_volume_24h)
        );

        let market = self.client.fetch_market_snapshot().await?;
        let symbols = market.symbols;
        info!(
            "พบเหรียญ USDT-M Perpetual ที่ผ่านเกณฑ์ทั้งหมด {} เหรียญ (สูงสุด {} เหรียญ)",
            symbols.len(),
            self.config.max_symbols
        );

        self.paper_trader.update_prices(&market.all_prices).await;

        {
            let mut t_guard = self.tickers.write().await;
            *t_guard = market.qualified_tickers;
        }

        let scan_15m_extra = self.config.kline_interval != "15m";
        let loaded_primary = self
            .warmup_interval_buffers(&symbols, &self.config.kline_interval)
            .await;
        info!(
            "โหลดข้อมูลแท่งเทียนย้อนหลัง ({}) สำเร็จ {}/{} เหรียญ",
            self.config.kline_interval,
            loaded_primary,
            symbols.len()
        );

        if scan_15m_extra {
            let loaded_15m = self.warmup_interval_buffers(&symbols, "15m").await;
            info!(
                "โหลดข้อมูลแท่งเทียนย้อนหลัง (15m Trigger) สำเร็จ {}/{} เหรียญ",
                loaded_15m,
                symbols.len()
            );
        } else {
            let primary_clone = self.candles.read().await.clone();
            let mut guard = self.candles_15m.write().await;
            *guard = primary_clone;
        }

        let mut signal_count = 0usize;
        if emit_initial_signals {
            signal_count += self.rescan_cached_mtf_buffers(&symbols).await;
        }

        // Scan Funding Rates & initialize Open Interest baseline
        let active_set: HashSet<String> = symbols.iter().cloned().collect();
        let fr_signals = self.scan_funding_rates(&active_set).await;
        signal_count += fr_signals;

        let oi_signals = self.scan_open_interest(&symbols).await;
        signal_count += oi_signals;

        self.save_state().await;
        if self.config.paper_trading_enabled {
            self.paper_trader.print_portfolio_summary().await;
        }

        Ok((symbols, signal_count))
    }

    /// Runs the full real-time scanner loop (WebSocket Kline Streams for primary TF + 15m + Liquidation Stream + Futures Metric Poller).
    pub async fn run_forever(self: Arc<Self>) -> Result<()> {
        let telegram_bot = self.telegram_bot_controller();
        telegram_bot.clone().spawn_background();

        let (symbols, initial_signals) = self.initialize_and_scan_once(true).await?;
        info!(
            "เริ่มต้นสแกนเรียลไทม์สำเร็จ! (พบสัญญาณรอบแรก {} รายการ)",
            initial_signals
        );

        telegram_bot
            .send_dashboard_message(false, Some("🚀 เริ่มทำงานสแกนตลาด + จำลองการเทรดแล้ว"))
            .await;

        let (kline_tx, mut kline_rx) = mpsc::channel::<WsKlineEvent>(4096);
        let (liq_tx, mut liq_rx) = mpsc::channel::<WsForceOrderEvent>(1024);

        // Split symbols into chunks of 100 per WebSocket connection for stability
        for chunk in symbols.chunks(100) {
            spawn_kline_ws_worker(
                self.config.ws_base_url.clone(),
                chunk.to_vec(),
                self.config.kline_interval.clone(),
                kline_tx.clone(),
            );
            if self.config.kline_interval != "15m" {
                spawn_kline_ws_worker(
                    self.config.ws_base_url.clone(),
                    chunk.to_vec(),
                    "15m".to_string(),
                    kline_tx.clone(),
                );
            }
        }

        // Spawn All-Market Liquidation stream (!forceOrder@arr)
        spawn_liquidation_ws_worker(self.config.ws_base_url.clone(), liq_tx);

        // Spawn periodic poller for MTF RAM rescan, Funding Rates, Open Interest Spikes, and 24hr Ticker refresh
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
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = scanner_poller.scan_now_notify.notified() => {
                        info!("🔍 กำลังสแกนตลาด MTF (5m+15m+1h) + Funding/OI ตามคำสั่ง Telegram...");
                    }
                }
                debug!("กำลังสแกน MTF จาก RAM พร้อมอัปเดต Funding Rate, Open Interest และ 24h Ticker...");

                if let Ok(market) = scanner_poller.client.fetch_market_snapshot().await {
                    scanner_poller.paper_trader.update_prices(&market.all_prices).await;

                    let mut t_guard = scanner_poller.tickers.write().await;
                    t_guard.extend(market.qualified_tickers);
                }

                scanner_poller
                    .rescan_cached_mtf_buffers(&tracked_symbols)
                    .await;
                scanner_poller.scan_funding_rates(&active_set).await;
                scanner_poller.scan_open_interest(&tracked_symbols).await;
                scanner_poller.save_state().await;
                scanner_poller.log_scan_heartbeat().await;
                if scanner_poller.config.paper_trading_enabled {
                    scanner_poller.paper_trader.print_portfolio_summary().await;
                }
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
            let tf_label = if self.config.kline_interval == "15m" {
                "15m".to_string()
            } else {
                format!("{} + 15m", self.config.kline_interval)
            };
            info!(
                "🟢 ระบบ Scanner-f กำลังทำงานแบบเรียลไทม์ ({} Trigger | กด Ctrl+C เพื่อหยุดการทำงาน)...",
                tf_label
            );
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
        let interval = ev.kline.interval.clone();
        let new_candle = ev.kline.to_candle();
        let symbol = ev.symbol;
        let is_primary = interval == self.config.kline_interval;
        let is_15m = interval == "15m";

        if !is_primary && !is_15m {
            return;
        }

        let snapshot = if is_primary {
            if !is_15m {
                self.metrics.ws_5m_ticks.fetch_add(1, Ordering::Relaxed);
                if new_candle.is_closed {
                    self.metrics.closed_5m_bars.fetch_add(1, Ordering::Relaxed);
                }
            }
            let mut guard = self.candles.write().await;
            let buf = guard.entry(symbol.clone()).or_insert_with(Vec::new);
            upsert_candle(buf, new_candle, self.config.candle_buffer_size);
            buf.clone()
        } else {
            Vec::new()
        };

        let snapshot_15m = if is_15m {
            self.metrics.ws_15m_ticks.fetch_add(1, Ordering::Relaxed);
            if new_candle.is_closed {
                self.metrics.closed_15m_bars.fetch_add(1, Ordering::Relaxed);
            }
            let mut guard = self.candles_15m.write().await;
            let buf = guard.entry(symbol.clone()).or_insert_with(Vec::new);
            upsert_candle(buf, new_candle, self.config.candle_buffer_size);
            buf.clone()
        } else {
            Vec::new()
        };

        if is_primary {
            self.paper_trader
                .on_price_tick(&symbol, new_candle.close)
                .await;
        }

        let eval_slice = if is_primary { &snapshot } else { &snapshot_15m };
        let signals = self
            .evaluate_symbol_candles_for_tf(&symbol, &interval, eval_slice, new_candle.is_closed)
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

        let (active_strategy, _) = self.paper_trader.get_effective_strategy_and_min_stars().await;
        if !active_strategy.allows_category(SignalCategory::LargeLiquidation) {
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
                ("🎯 กลยุทธ์".to_string(), active_strategy.title_th().to_string()),
                ("มูลค่า Liquidation".to_string(), format!("${}", format_compact_usd(usd_val))),
                ("จำนวน (Qty)".to_string(), format!("{:.4}", order_qty)),
                ("ราคาเฉลี่ย".to_string(), format!("${}", format_price(order_price))),
            ],
            timestamp: Utc::now(),
        };

        self.notifier.dispatch(signal).await;
    }

    /// Evaluates price action, volume spikes, and technical indicators on a symbol's candle buffer for a given trigger timeframe.
    async fn evaluate_symbol_candles_for_tf(
        &self,
        symbol: &str,
        trigger_tf: &str,
        candles: &[Candle],
        check_closed_indicators: bool,
    ) -> Vec<ScanSignal> {
        let mut signals = Vec::new();
        if candles.len() < 5 {
            return signals;
        }

        let (active_strategy, eff_min_stars) =
            self.paper_trader.get_effective_strategy_and_min_stars().await;

        let latest = match candles.last() {
            Some(c) => *c,
            None => return signals,
        };

        let (change_24h, vol_24h) = self.get_ticker_meta(symbol).await;
        let tf = trigger_tf;

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
                timeframe: tf.to_string(),
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
                timeframe: tf.to_string(),
                metric_summary: format!("ราคาทุบ {:.2}% ในแท่ง {}", candle_chg, tf),
                details,
                timestamp: Utc::now(),
            });
        }

        // 2. Real-time Volume Spike check (require >= 0.6% directional move to avoid flat doji noise)
        if let Some((vol_ratio, avg_vol)) = vol_ratio_opt {
            if vol_ratio >= self.config.volume_spike_multiplier
                && candle_chg.abs() >= VOLUME_SPIKE_MIN_MOVE_PCT
            {
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
                    timeframe: tf.to_string(),
                    metric_summary: format!(
                        "[{}] วอลุ่มพุ่ง {:.2} เท่าของค่าเฉลี่ย {} แท่ง ({:+.2}%)",
                        tf, vol_ratio, self.config.volume_lookback, candle_chg
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
        if check_closed_indicators {
            // Use completed candles for indicator evaluation
            let eval_slice =
                if candles.last().map(|c| !c.is_closed).unwrap_or(false) && candles.len() > 1 {
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
                        timeframe: tf.to_string(),
                        metric_summary: format!(
                            "[{}] RSI({}) = {:.2} (Overbought >= {:.0})",
                            tf, self.config.rsi_period, rsi, self.config.rsi_overbought
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
                        timeframe: tf.to_string(),
                        metric_summary: format!(
                            "[{}] RSI({}) = {:.2} (Oversold <= {:.0})",
                            tf, self.config.rsi_period, rsi, self.config.rsi_oversold
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
                            timeframe: tf.to_string(),
                            metric_summary: format!(
                                "[{}] EMA({}) ตัดขึ้นเหนือ EMA({}) (Golden Cross)",
                                tf, self.config.ema_fast_period, self.config.ema_slow_period
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
                            timeframe: tf.to_string(),
                            metric_summary: format!(
                                "[{}] EMA({}) ตัดลงต่ำกว่า EMA({}) (Death Cross)",
                                tf, self.config.ema_fast_period, self.config.ema_slow_period
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
                        timeframe: tf.to_string(),
                        metric_summary: format!(
                            "[{}] ราคาปิดทะลุ Upper Bollinger Band พร้อมวอลุ่ม {:.2}x",
                            tf, eval_vol_ratio
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
                        timeframe: tf.to_string(),
                        metric_summary: format!(
                            "[{}] ราคาปิดหลุด Lower Bollinger Band พร้อมวอลุ่ม {:.2}x",
                            tf, eval_vol_ratio
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
        }

        if signals.is_empty() {
            return signals;
        }

        // On mid-candle ticks, throttle repeated MTF checks per (symbol, tf, category) to once every 30s
        // so high-frequency WS updates don't inflate raw_triggers or repeat failed MTF checks on every tick,
        // while still allowing mid-candle Pump/Dump/VolumeSpike triggers to pass full MTF & open Paper Trades.
        if !check_closed_indicators {
            let mut throttled = Vec::with_capacity(signals.len());
            for sig in signals {
                if self.notifier.is_in_cooldown(&sig.symbol, sig.category).await {
                    continue;
                }
                if self
                    .should_allow_mid_candle_eval(&sig.symbol, trigger_tf, sig.category)
                    .await
                {
                    throttled.push(sig);
                }
            }
            if throttled.is_empty() {
                return throttled;
            }
            signals = throttled;
        }

        self.metrics
            .raw_triggers
            .fetch_add(signals.len() as u64, Ordering::Relaxed);

        // Apply active strategy category filter before cooldown/MTF lookup
        signals.retain(|s| active_strategy.allows_category(s.category));
        if signals.is_empty() {
            return signals;
        }

        // Filter out signals still inside cooldown before fetching 1h/MTF context or opening paper positions
        if check_closed_indicators {
            let mut active_signals = Vec::with_capacity(signals.len());
            for sig in signals {
                if !self.notifier.is_in_cooldown(&sig.symbol, sig.category).await {
                    active_signals.push(sig);
                }
            }
            if active_signals.is_empty() {
                return active_signals;
            }
            signals = active_signals;
        }

        // Reuse in-memory 5m/15m buffers and 300s-cached 1h candles for Multi-Timeframe Confluence
        let (res_5m, res_15m, res_1h) = if trigger_tf == "15m" {
            let (r5, r1h) = tokio::join!(
                self.get_or_fetch_candles_5m(symbol),
                self.fetch_klines_1h_cached(symbol)
            );
            (r5, Ok(candles.to_vec()), r1h)
        } else if trigger_tf == "5m" {
            let (r15, r1h) = tokio::join!(
                self.get_or_fetch_candles_15m(symbol),
                self.fetch_klines_1h_cached(symbol)
            );
            (Ok(candles.to_vec()), r15, r1h)
        } else {
            tokio::join!(
                self.get_or_fetch_candles_5m(symbol),
                self.get_or_fetch_candles_15m(symbol),
                self.fetch_klines_1h_cached(symbol)
            )
        };

        let mtf_label = if trigger_tf == "15m" {
            "15m+5m+1h".to_string()
        } else {
            format!("{}+15m+1h", trigger_tf)
        };

        if let (Ok(candles_5m), Ok(candles_15m), Ok(candles_1h)) = (res_5m, res_15m, res_1h) {
            let state_5m_opt = analyze_timeframe_state(
                &candles_5m,
                self.config.rsi_period,
                self.config.rsi_overbought,
                self.config.rsi_oversold,
                self.config.volume_lookback,
            );
            let state_15m_opt = analyze_timeframe_state(
                &candles_15m,
                self.config.rsi_period,
                self.config.rsi_overbought,
                self.config.rsi_oversold,
                self.config.volume_lookback,
            );
            let state_1h_opt = analyze_timeframe_state(
                &candles_1h,
                self.config.rsi_period,
                self.config.rsi_overbought,
                self.config.rsi_oversold,
                self.config.volume_lookback,
            );

            if let (Some(state_5m), Some(state_15m), Some(state_1h)) =
                (state_5m_opt, state_15m_opt, state_1h_opt)
            {
                let mut mtf_filtered = Vec::new();
                for mut sig in signals {
                    let is_reversal = matches!(
                        sig.category,
                        SignalCategory::RsiOverbought
                            | SignalCategory::RsiOversold
                            | SignalCategory::FundingRateExtreme
                    );
                    let (stars, grade_str, aligned_1h, reasons) = evaluate_mtf_confluence(
                        sig.direction,
                        is_reversal,
                        &state_5m,
                        &state_15m,
                        &state_1h,
                        self.config.rsi_overbought,
                        self.config.rsi_oversold,
                    );
                    let passes_1h = !active_strategy.requires_1h_alignment() || aligned_1h;
                    if stars >= eff_min_stars && passes_1h {
                        sig.timeframe = mtf_label.clone();
                        if let Some(levels) = append_mtf_trade_plan(
                            &mut sig.details,
                            sig.direction,
                            sig.price,
                            &state_5m,
                            &state_15m,
                            &state_1h,
                            &candles_15m,
                            &grade_str,
                            aligned_1h,
                            &reasons,
                            active_strategy,
                        ) {
                            self.paper_trader
                                .try_open_position(
                                    &sig.symbol,
                                    sig.direction,
                                    sig.category,
                                    &sig.timeframe,
                                    stars,
                                    &levels,
                                )
                                .await;
                        }
                        mtf_filtered.push(sig);
                    }
                }
                if !mtf_filtered.is_empty() {
                    self.metrics
                        .passed_signals
                        .fetch_add(mtf_filtered.len() as u64, Ordering::Relaxed);
                }
                return mtf_filtered;
            }
        }

        if active_strategy.requires_1h_alignment() || eff_min_stars >= 3 {
            return Vec::new();
        }

        for sig in &mut signals {
            sig.details
                .push(("🎯 กลยุทธ์".to_string(), active_strategy.title_th().to_string()));
            append_trade_plan(&mut sig.details, sig.direction, sig.price, Some(candles));
        }

        if !signals.is_empty() {
            self.metrics
                .passed_signals
                .fetch_add(signals.len() as u64, Ordering::Relaxed);
        }
        signals
    }

    /// Scans all active symbols for extreme Funding Rates (`/fapi/v1/premiumIndex`).
    async fn scan_funding_rates(&self, active_symbols: &HashSet<String>) -> usize {
        let (active_strategy, eff_min_stars) =
            self.paper_trader.get_effective_strategy_and_min_stars().await;
        if !active_strategy.allows_category(SignalCategory::FundingRateExtreme) {
            return 0;
        }

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

            if self
                .notifier
                .is_in_cooldown(&item.symbol, SignalCategory::FundingRateExtreme)
                .await
            {
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

            let (res_5m, res_15m, res_1h) = tokio::join!(
                self.get_or_fetch_candles_5m(&item.symbol),
                self.get_or_fetch_candles_15m(&item.symbol),
                self.fetch_klines_1h_cached(&item.symbol)
            );

            let mut timeframe_label = "Funding (5m+15m+1h)".to_string();
            let mut mtf_ready = false;

            if let (Ok(candles_5m), Ok(candles_15m), Ok(candles_1h)) = (res_5m, res_15m, res_1h) {
                let s5_opt = analyze_timeframe_state(
                    &candles_5m,
                    self.config.rsi_period,
                    self.config.rsi_overbought,
                    self.config.rsi_oversold,
                    self.config.volume_lookback,
                );
                let s15_opt = analyze_timeframe_state(
                    &candles_15m,
                    self.config.rsi_period,
                    self.config.rsi_overbought,
                    self.config.rsi_oversold,
                    self.config.volume_lookback,
                );
                let s1h_opt = analyze_timeframe_state(
                    &candles_1h,
                    self.config.rsi_period,
                    self.config.rsi_overbought,
                    self.config.rsi_oversold,
                    self.config.volume_lookback,
                );

                if let (Some(state_5m), Some(state_15m), Some(state_1h)) =
                    (s5_opt, s15_opt, s1h_opt)
                {
                    let (stars, grade_str, aligned_1h, reasons) = evaluate_mtf_confluence(
                        direction,
                        true,
                        &state_5m,
                        &state_15m,
                        &state_1h,
                        self.config.rsi_overbought,
                        self.config.rsi_oversold,
                    );
                    let passes_1h = !active_strategy.requires_1h_alignment() || aligned_1h;
                    if stars < eff_min_stars || !passes_1h {
                        continue;
                    }
                    if let Some(levels) = append_mtf_trade_plan(
                        &mut details,
                        direction,
                        mark_price,
                        &state_5m,
                        &state_15m,
                        &state_1h,
                        &candles_15m,
                        &grade_str,
                        aligned_1h,
                        &reasons,
                        active_strategy,
                    ) {
                        self.paper_trader
                            .try_open_position(
                                &item.symbol,
                                direction,
                                SignalCategory::FundingRateExtreme,
                                &timeframe_label,
                                stars,
                                &levels,
                            )
                            .await;
                    }
                    mtf_ready = true;
                }
            }

            if !mtf_ready {
                if active_strategy.requires_1h_alignment() || eff_min_stars >= 3 {
                    continue;
                }
                timeframe_label = "Funding".to_string();
                details.push(("🎯 กลยุทธ์".to_string(), active_strategy.title_th().to_string()));
                append_trade_plan(&mut details, direction, mark_price, None);
            }

            let sig = ScanSignal {
                symbol: item.symbol,
                category: SignalCategory::FundingRateExtreme,
                direction,
                price: mark_price,
                change_24h_pct: change_24h,
                quote_volume_24h: vol_24h,
                timeframe: timeframe_label,
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
        let (active_strategy, _) = self.paper_trader.get_effective_strategy_and_min_stars().await;
        let allow_oi_alerts = active_strategy.allows_category(SignalCategory::OpenInterestSpike);

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

                if !allow_oi_alerts {
                    continue;
                }

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
                                    ("🎯 กลยุทธ์".to_string(), active_strategy.title_th().to_string()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::WsKlineData;
    use crate::strategy::TradingStrategy;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_scanner_config(tag: &str, strategy: TradingStrategy) -> ScannerConfig {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let tmp = std::env::temp_dir();
        let mut cfg = ScannerConfig::from_env();
        cfg.kline_interval = "5m".to_string();
        cfg.candle_buffer_size = 100;
        cfg.scanner_strategy = strategy;
        cfg.price_change_threshold_pct = 2.5;
        cfg.volume_spike_multiplier = 3.5;
        cfg.volume_lookback = 20;
        cfg.rsi_period = 14;
        cfg.rsi_overbought = 75.0;
        cfg.rsi_oversold = 25.0;
        cfg.ema_fast_period = 9;
        cfg.ema_slow_period = 21;
        cfg.bb_period = 20;
        cfg.bb_std_dev = 2.0;
        cfg.bb_min_volume_ratio = 1.8;
        cfg.alert_cooldown_secs = 900;
        cfg.state_file = tmp
            .join(format!("scanner_f_state_{}_{}.json", tag, nanos))
            .to_string_lossy()
            .into_owned();
        cfg.paper_trading_enabled = true;
        cfg.paper_initial_balance = 1000.0;
        cfg.paper_leverage = 10.0;
        cfg.paper_position_pct = 10.0;
        cfg.paper_max_open_positions = 5;
        cfg.paper_min_stars = 2;
        cfg.paper_taker_fee_pct = 0.05;
        cfg.paper_notify_webhooks = false;
        cfg.paper_telegram_bot_enabled = false;
        cfg.paper_state_file = tmp
            .join(format!("scanner_f_paper_{}_{}.json", tag, nanos))
            .to_string_lossy()
            .into_owned();
        cfg.paper_trades_csv = tmp
            .join(format!("scanner_f_paper_{}_{}.csv", tag, nanos))
            .to_string_lossy()
            .into_owned();
        cfg.telegram_bot_token = None;
        cfg.telegram_chat_id = None;
        cfg.discord_webhook_url = None;
        cfg
    }

    fn make_candle(open_time: i64, open: f64, close: f64, is_closed: bool) -> Candle {
        let high = open.max(close) + 0.15;
        let low = (open.min(close) - 0.15).max(0.01);
        Candle {
            open_time,
            open,
            high,
            low,
            close,
            volume: 1_000_000.0 / close.max(1e-6),
            quote_volume: 1_000_000.0,
            is_closed,
        }
    }

    fn build_bullish_steady_candles(count: usize, step_ms: i64) -> Vec<Candle> {
        let mut candles = Vec::with_capacity(count);
        let mut price = 100.0;
        for i in 0..count {
            let delta = if i % 2 == 0 { 0.35 } else { -0.20 };
            let open = price;
            price += delta;
            candles.push(make_candle(
                (i as i64 + 1) * step_ms,
                open,
                price,
                true,
            ));
        }
        // End on an upward step so close >= ema21 and ema9 > ema21
        if candles.last().is_some_and(|c| c.close < c.open) {
            let last_open = candles.last().unwrap().close;
            candles.push(make_candle(
                (count as i64 + 1) * step_ms,
                last_open,
                last_open + 0.35,
                true,
            ));
        }
        candles
    }

    fn build_bearish_steady_candles(count: usize, step_ms: i64) -> Vec<Candle> {
        let mut candles = Vec::with_capacity(count);
        let mut price = 120.0;
        for i in 0..count {
            let delta = if i % 2 == 0 { -0.35 } else { 0.20 };
            let open = price;
            price += delta;
            candles.push(make_candle(
                (i as i64 + 1) * step_ms,
                open,
                price,
                true,
            ));
        }
        if candles.last().is_some_and(|c| c.close > c.open) {
            let last_open = candles.last().unwrap().close;
            candles.push(make_candle(
                (count as i64 + 1) * step_ms,
                last_open,
                last_open - 0.35,
                true,
            ));
        }
        candles
    }

    fn build_bullish_ema_cross_candles(step_ms: i64) -> Vec<Candle> {
        let mut closes = Vec::with_capacity(60);
        for i in 0..26 {
            let c = if i % 2 == 0 { 105.3 } else { 104.7 };
            closes.push(c);
        }
        let mut price = 105.0;
        for i in 0..30 {
            price += if i % 2 == 0 { -0.5 } else { 0.3 };
            closes.push(price);
        }
        closes.push(102.7);
        closes.push(103.4);
        closes.push(104.2);
        closes.push(105.6);

        let mut candles = Vec::with_capacity(closes.len());
        let mut prev_close = closes[0];
        for (i, &c) in closes.iter().enumerate() {
            candles.push(make_candle(
                (i as i64 + 1) * step_ms,
                prev_close,
                c,
                true,
            ));
            prev_close = c;
        }
        candles
    }

    fn build_rsi_oversold_candles(step_ms: i64) -> Vec<Candle> {
        let mut candles = Vec::with_capacity(60);
        let mut price = 120.0;
        for i in 0..60 {
            let open = price;
            price -= 0.45;
            candles.push(make_candle(
                (i as i64 + 1) * step_ms,
                open,
                price,
                true,
            ));
        }
        candles
    }

    #[test]
    fn test_upsert_candle_update_rollover_and_truncation() {
        let mut buf = Vec::new();
        upsert_candle(&mut buf, make_candle(1000, 10.0, 10.5, false), 3);
        assert_eq!(buf.len(), 1);
        assert!(!buf[0].is_closed);

        // In-place update for same open_time
        upsert_candle(&mut buf, make_candle(1000, 10.0, 11.2, false), 3);
        assert_eq!(buf.len(), 1);
        assert!((buf[0].close - 11.2).abs() < 1e-9);

        // Stale candle with earlier open_time is ignored
        upsert_candle(&mut buf, make_candle(500, 9.0, 9.5, true), 3);
        assert_eq!(buf.len(), 1);

        // Newer candle marks previous closed and appends
        upsert_candle(&mut buf, make_candle(2000, 11.2, 11.8, false), 3);
        assert_eq!(buf.len(), 2);
        assert!(buf[0].is_closed);
        assert!(!buf[1].is_closed);

        // Exceeding max_len truncates oldest entries
        upsert_candle(&mut buf, make_candle(3000, 11.8, 12.0, false), 3);
        upsert_candle(&mut buf, make_candle(4000, 12.0, 12.4, false), 3);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf[0].open_time, 2000);
        assert_eq!(buf[2].open_time, 4000);
    }

    #[tokio::test]
    async fn test_handle_kline_event_routes_5m_and_15m_buffers_and_metrics() {
        let cfg = test_scanner_config("ws_route", TradingStrategy::TrendFollow);
        let notifier = AlertNotifier::new(cfg.clone());
        let pt = PaperTrader::new(cfg.clone(), notifier.clone());
        let scanner = FuturesScanner::new(cfg.clone(), notifier, pt);

        scanner
            .handle_kline_event(WsKlineEvent {
                symbol: "BTCUSDT".to_string(),
                kline: WsKlineData {
                    open_time: 1000,
                    close_time: 1299,
                    interval: "5m".to_string(),
                    open: "100.0".to_string(),
                    close: "100.5".to_string(),
                    high: "100.6".to_string(),
                    low: "99.9".to_string(),
                    volume: "10.0".to_string(),
                    quote_volume: "1000.0".to_string(),
                    is_closed: true,
                },
            })
            .await;

        scanner
            .handle_kline_event(WsKlineEvent {
                symbol: "BTCUSDT".to_string(),
                kline: WsKlineData {
                    open_time: 2000,
                    close_time: 2899,
                    interval: "15m".to_string(),
                    open: "100.0".to_string(),
                    close: "101.0".to_string(),
                    high: "101.2".to_string(),
                    low: "99.8".to_string(),
                    volume: "20.0".to_string(),
                    quote_volume: "2000.0".to_string(),
                    is_closed: false,
                },
            })
            .await;

        assert_eq!(scanner.metrics.ws_5m_ticks.load(Ordering::Relaxed), 1);
        assert_eq!(scanner.metrics.closed_5m_bars.load(Ordering::Relaxed), 1);
        assert_eq!(scanner.metrics.ws_15m_ticks.load(Ordering::Relaxed), 1);
        assert_eq!(scanner.metrics.closed_15m_bars.load(Ordering::Relaxed), 0);

        {
            let g5 = scanner.candles.read().await;
            assert_eq!(g5.get("BTCUSDT").map(|v| v.len()), Some(1));
        }
        {
            let g15 = scanner.candles_15m.read().await;
            assert_eq!(g15.get("BTCUSDT").map(|v| v.len()), Some(1));
        }

        let _ = fs::remove_file(&cfg.state_file);
        let _ = fs::remove_file(&cfg.paper_state_file);
        let _ = fs::remove_file(&cfg.paper_trades_csv);
    }

    #[tokio::test]
    async fn test_15m_trigger_trend_follow_mtf_cooldown_and_filters() {
        let cfg = test_scanner_config("tf15m", TradingStrategy::TrendFollow);
        let notifier = AlertNotifier::new(cfg.clone());
        let pt = PaperTrader::new(cfg.clone(), notifier.clone());
        let scanner = FuturesScanner::new(cfg.clone(), notifier, pt);

        let candles_5m = build_bullish_steady_candles(60, 300_000);
        let candles_15m_cross = build_bullish_ema_cross_candles(900_000);
        let candles_1h_bull = build_bullish_steady_candles(60, 3_600_000);
        let candles_1h_bear = build_bearish_steady_candles(60, 3_600_000);
        let candles_15m_oversold = build_rsi_oversold_candles(900_000);

        let closes_15m: Vec<f64> = candles_15m_cross.iter().map(|c| c.close).collect();
        let ema_check = detect_ema_crossover(&closes_15m, 9, 21).unwrap();
        assert_eq!(ema_check.crossover, Some(CrossoverType::Bullish));

        let now_ts = Utc::now().timestamp();
        {
            let mut g5 = scanner.candles.write().await;
            g5.insert("BTCUSDT".to_string(), candles_5m.clone());
            g5.insert("ETHUSDT".to_string(), candles_5m.clone());
            g5.insert("SOLUSDT".to_string(), candles_5m);
        }
        {
            let mut g1h = scanner.candles_1h_cache.write().await;
            g1h.insert("BTCUSDT".to_string(), (now_ts, candles_1h_bull.clone()));
            g1h.insert("ETHUSDT".to_string(), (now_ts, candles_1h_bear));
            g1h.insert("SOLUSDT".to_string(), (now_ts, candles_1h_bull));
        }

        // 1) Aligned 1h + 3 stars on 15m EMA Golden Cross passes TrendFollow and opens paper trade
        let passed_btc = scanner
            .evaluate_symbol_candles_for_tf("BTCUSDT", "15m", &candles_15m_cross, true)
            .await;
        assert_eq!(passed_btc.len(), 1);
        assert_eq!(passed_btc[0].category, SignalCategory::EmaBullishCross);
        assert_eq!(passed_btc[0].timeframe, "15m+5m+1h");
        assert!(passed_btc[0].metric_summary.contains("[15m]"));
        assert_eq!(scanner.metrics.raw_triggers.load(Ordering::Relaxed), 1);
        assert_eq!(scanner.metrics.passed_signals.load(Ordering::Relaxed), 1);

        // Dispatch records cooldown; verify TREND_FOLLOW paper trade opened with 25/35/40 splits
        scanner
            .notifier
            .dispatch(passed_btc.into_iter().next().unwrap())
            .await;
        let snap = scanner.paper_trader.get_state_snapshot().await;
        assert_eq!(snap.open_positions.len(), 1);
        assert_eq!(snap.open_positions[0].symbol, "BTCUSDT");
        assert_eq!(snap.open_positions[0].strategy, "TREND_FOLLOW");
        assert_eq!(snap.open_positions[0].timeframe, "15m+5m+1h");
        assert_eq!(snap.open_positions[0].stars, 3);
        assert_eq!(snap.open_positions[0].tp_close_splits(), (25.0, 35.0, 40.0));

        // 2) Immediate re-evaluation hits cooldown without duplicating signals or paper trades
        let duplicate_btc = scanner
            .evaluate_symbol_candles_for_tf("BTCUSDT", "15m", &candles_15m_cross, true)
            .await;
        assert!(duplicate_btc.is_empty());
        assert_eq!(scanner.metrics.passed_signals.load(Ordering::Relaxed), 1);

        // 3) Bearish 1h blocks 15m bullish EMA cross under TrendFollow (requires 1h alignment)
        let blocked_eth = scanner
            .evaluate_symbol_candles_for_tf("ETHUSDT", "15m", &candles_15m_cross, true)
            .await;
        assert!(blocked_eth.is_empty());
        assert_eq!(scanner.paper_trader.get_state_snapshot().await.open_positions.len(), 1);
        assert_eq!(scanner.metrics.passed_signals.load(Ordering::Relaxed), 1);

        // 4) Non-TrendFollow trigger (RsiOversold) is filtered out by TrendFollow strategy category gate
        let filtered_sol = scanner
            .evaluate_symbol_candles_for_tf("SOLUSDT", "15m", &candles_15m_oversold, true)
            .await;
        assert!(filtered_sol.is_empty());
        assert_eq!(scanner.metrics.passed_signals.load(Ordering::Relaxed), 1);
        assert!(scanner.metrics.raw_triggers.load(Ordering::Relaxed) >= 4);

        let _ = fs::remove_file(&cfg.state_file);
        let _ = fs::remove_file(&cfg.paper_state_file);
        let _ = fs::remove_file(&cfg.paper_trades_csv);
    }

    #[tokio::test]
    async fn test_mid_candle_volume_spike_mtf_sharpness_and_throttle() {
        let cfg = test_scanner_config("mid_mtf", TradingStrategy::TrendFollow);
        let notifier = AlertNotifier::new(cfg.clone());
        let pt = PaperTrader::new(cfg.clone(), notifier.clone());
        let scanner = FuturesScanner::new(cfg.clone(), notifier, pt);

        let candles_5m_base = build_bullish_steady_candles(59, 300_000);
        let last_base = *candles_5m_base.last().unwrap();
        let next_open_time = last_base.open_time + 300_000;

        // Unclosed flat doji (+0.10%) with 5x volume -> should be filtered out (< 0.6% move)
        let mut doji_5m = candles_5m_base.clone();
        let mut doji_candle = make_candle(
            next_open_time,
            last_base.close,
            last_base.close * 1.001,
            false,
        );
        doji_candle.quote_volume = 5_000_000.0;
        doji_5m.push(doji_candle);

        // Unclosed directional candle (+1.20%) with 5x volume -> valid VolumeSpike
        let mut spike_5m = candles_5m_base;
        let mut spike_candle = make_candle(
            next_open_time,
            last_base.close,
            last_base.close * 1.012,
            false,
        );
        spike_candle.quote_volume = 5_000_000.0;
        spike_5m.push(spike_candle);

        let candles_15m_bull = build_bullish_steady_candles(60, 900_000);
        let candles_1h_bull = build_bullish_steady_candles(60, 3_600_000);
        let candles_1h_bear = build_bearish_steady_candles(60, 3_600_000);
        let now_ts = Utc::now().timestamp();

        {
            let mut g15 = scanner.candles_15m.write().await;
            g15.insert("BTCUSDT".to_string(), candles_15m_bull.clone());
            g15.insert("ETHUSDT".to_string(), candles_15m_bull);
        }
        {
            let mut g1h = scanner.candles_1h_cache.write().await;
            g1h.insert("BTCUSDT".to_string(), (now_ts, candles_1h_bull));
            g1h.insert("ETHUSDT".to_string(), (now_ts, candles_1h_bear));
        }

        // 1) Flat doji with 5x volume is rejected before raw_triggers
        let flat_res = scanner
            .evaluate_symbol_candles_for_tf("BTCUSDT", "5m", &doji_5m, false)
            .await;
        assert!(flat_res.is_empty());
        assert_eq!(scanner.metrics.raw_triggers.load(Ordering::Relaxed), 0);

        // 2) Directional mid-candle VolumeSpike with bearish 1h fails MTF without burning cooldown
        let eth_fail_1 = scanner
            .evaluate_symbol_candles_for_tf("ETHUSDT", "5m", &spike_5m, false)
            .await;
        assert!(eth_fail_1.is_empty());
        assert_eq!(scanner.metrics.raw_triggers.load(Ordering::Relaxed), 1);
        assert!(
            !scanner
                .notifier
                .is_in_cooldown("ETHUSDT", SignalCategory::VolumeSpike)
                .await
        );

        // 3) Immediate second mid-candle tick is suppressed by the 30s throttle
        let eth_throttled = scanner
            .evaluate_symbol_candles_for_tf("ETHUSDT", "5m", &spike_5m, false)
            .await;
        assert!(eth_throttled.is_empty());
        assert_eq!(scanner.metrics.raw_triggers.load(Ordering::Relaxed), 1);

        // 4) Bar-close evaluation bypasses mid-candle throttle immediately
        let eth_close = scanner
            .evaluate_symbol_candles_for_tf("ETHUSDT", "5m", &spike_5m, true)
            .await;
        assert!(eth_close.is_empty());
        assert_eq!(scanner.metrics.raw_triggers.load(Ordering::Relaxed), 2);

        // 5) Aligned 5m+15m+1h mid-candle VolumeSpike passes TrendFollow (3⭐ + 1h) and opens Paper Trade
        let btc_pass = scanner
            .evaluate_symbol_candles_for_tf("BTCUSDT", "5m", &spike_5m, false)
            .await;
        assert_eq!(btc_pass.len(), 1);
        assert_eq!(btc_pass[0].category, SignalCategory::VolumeSpike);
        assert_eq!(btc_pass[0].timeframe, "5m+15m+1h");
        assert_eq!(scanner.metrics.passed_signals.load(Ordering::Relaxed), 1);

        let snap = scanner.paper_trader.get_state_snapshot().await;
        assert_eq!(snap.open_positions.len(), 1);
        assert_eq!(snap.open_positions[0].symbol, "BTCUSDT");
        assert_eq!(snap.open_positions[0].strategy, "TREND_FOLLOW");
        assert_eq!(snap.open_positions[0].timeframe, "5m+15m+1h");
        assert_eq!(snap.open_positions[0].stars, 3);

        let _ = fs::remove_file(&cfg.state_file);
        let _ = fs::remove_file(&cfg.paper_state_file);
        let _ = fs::remove_file(&cfg.paper_trades_csv);
    }

    #[tokio::test]
    async fn test_rescan_cached_mtf_buffers_catches_1h_alignment_flip() {
        let cfg = test_scanner_config("rescan_mtf", TradingStrategy::TrendFollow);
        let notifier = AlertNotifier::new(cfg.clone());
        let pt = PaperTrader::new(cfg.clone(), notifier.clone());
        let scanner = FuturesScanner::new(cfg.clone(), notifier, pt);

        let candles_5m = build_bullish_steady_candles(60, 300_000);
        // Simulate live 15m buffer: closed EMA Golden Cross bar followed by an unclosed live candle
        let mut candles_15m = build_bullish_ema_cross_candles(900_000);
        let last_15m = *candles_15m.last().unwrap();
        candles_15m.push(make_candle(
            last_15m.open_time + 900_000,
            last_15m.close,
            last_15m.close + 0.05,
            false,
        ));
        let candles_1h_bear = build_bearish_steady_candles(60, 3_600_000);
        let candles_1h_bull = build_bullish_steady_candles(60, 3_600_000);
        let now_ts = Utc::now().timestamp();

        {
            let mut g5 = scanner.candles.write().await;
            g5.insert("BTCUSDT".to_string(), candles_5m);
        }
        {
            let mut g15 = scanner.candles_15m.write().await;
            g15.insert("BTCUSDT".to_string(), candles_15m);
        }
        {
            let mut g1h = scanner.candles_1h_cache.write().await;
            g1h.insert("BTCUSDT".to_string(), (now_ts, candles_1h_bear));
        }

        let symbols = vec!["BTCUSDT".to_string()];

        // 1) Bearish 1h blocks the closed-bar 15m EMA cross on rescan
        let count_blocked = scanner.rescan_cached_mtf_buffers(&symbols).await;
        assert_eq!(count_blocked, 0);
        assert!(
            scanner
                .paper_trader
                .get_state_snapshot()
                .await
                .open_positions
                .is_empty()
        );

        // 2) Once 1h flips bullish, the 60s in-RAM rescan catches the setup and opens a Paper Trade
        {
            let mut g1h = scanner.candles_1h_cache.write().await;
            g1h.insert("BTCUSDT".to_string(), (now_ts, candles_1h_bull));
        }
        let count_caught = scanner.rescan_cached_mtf_buffers(&symbols).await;
        assert_eq!(count_caught, 1);
        let snap = scanner.paper_trader.get_state_snapshot().await;
        assert_eq!(snap.open_positions.len(), 1);
        assert_eq!(snap.open_positions[0].symbol, "BTCUSDT");
        assert_eq!(snap.open_positions[0].strategy, "TREND_FOLLOW");
        assert_eq!(snap.open_positions[0].timeframe, "15m+5m+1h");

        // 3) Next rescan respects cooldown without duplicating signal or trade
        let count_dup = scanner.rescan_cached_mtf_buffers(&symbols).await;
        assert_eq!(count_dup, 0);
        assert_eq!(
            scanner
                .paper_trader
                .get_state_snapshot()
                .await
                .open_positions
                .len(),
            1
        );

        let _ = fs::remove_file(&cfg.state_file);
        let _ = fs::remove_file(&cfg.paper_state_file);
        let _ = fs::remove_file(&cfg.paper_trades_csv);
    }
}
