use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::config::ScannerConfig;
use crate::indicators::MtfTradeLevels;
use crate::models::{SignalCategory, SignalDirection};
use crate::notifier::{format_price, AlertNotifier};
use crate::strategy::{record_strategy_closed_trade, StrategyPerfStats, TradingStrategy};

const CSV_HEADER: &str = "timestamp,trade_id,event,symbol,side,stars,timeframe,signal,entry_price,exec_price,closed_pct,margin_used,leverage,gross_pnl_usdt,fee_usdt,net_pnl_usdt,balance_after_usdt,equity_after_usdt,note,strategy\n";

fn default_position_strategy() -> String {
    TradingStrategy::Balanced.key().to_string()
}

fn default_tp1_close_pct() -> f64 {
    40.0
}

fn default_tp2_close_pct() -> f64 {
    30.0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PaperSide {
    Long,
    Short,
}

impl PaperSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Long => "LONG",
            Self::Short => "SHORT",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPosition {
    pub id: u64,
    pub symbol: String,
    pub side: PaperSide,
    pub stars: u8,
    pub signal_category: String,
    pub timeframe: String,
    #[serde(default = "default_position_strategy")]
    pub strategy: String,
    #[serde(default = "default_tp1_close_pct")]
    pub tp1_close_pct: f64,
    #[serde(default = "default_tp2_close_pct")]
    pub tp2_close_pct: f64,
    pub entry_price: f64,
    pub initial_sl: f64,
    pub current_sl: f64,
    pub tp1: f64,
    pub tp2: f64,
    pub tp3: f64,
    pub leverage: f64,
    pub initial_margin: f64,
    pub remaining_margin: f64,
    pub initial_qty: f64,
    pub remaining_qty: f64,
    pub remaining_fraction: f64,
    pub tp1_hit: bool,
    pub tp2_hit: bool,
    pub realized_pnl_trade: f64,
    pub fees_paid_trade: f64,
    pub last_price: f64,
    pub opened_at: String,
}

impl PaperPosition {
    pub fn tp_close_splits(&self) -> (f64, f64, f64) {
        let tp1 = if self.tp1_close_pct.is_finite() {
            self.tp1_close_pct.clamp(10.0, 80.0)
        } else {
            40.0
        };
        let tp2 = if self.tp2_close_pct.is_finite() {
            self.tp2_close_pct.clamp(10.0, 90.0 - tp1)
        } else {
            30.0
        };
        let tp3 = (100.0 - tp1 - tp2).max(5.0);
        (tp1, tp2, tp3)
    }

    pub fn unrealized_pnl(&self) -> f64 {
        match self.side {
            PaperSide::Long => (self.last_price - self.entry_price) * self.remaining_qty,
            PaperSide::Short => (self.entry_price - self.last_price) * self.remaining_qty,
        }
    }

    pub fn unrealized_roe_pct(&self) -> f64 {
        if self.remaining_margin <= 0.0 {
            0.0
        } else {
            (self.unrealized_pnl() / self.remaining_margin) * 100.0
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPortfolioState {
    pub initial_balance: f64,
    pub free_balance: f64,
    pub realized_pnl: f64,
    pub total_fees: f64,
    pub peak_equity: f64,
    pub max_drawdown_pct: f64,
    pub next_trade_id: u64,
    pub total_closed: u64,
    pub win_count: u64,
    pub loss_count: u64,
    #[serde(default)]
    pub paused_opening: bool,
    #[serde(default)]
    pub custom_leverage: Option<f64>,
    #[serde(default)]
    pub custom_position_pct: Option<f64>,
    #[serde(default)]
    pub custom_max_open_positions: Option<usize>,
    #[serde(default)]
    pub custom_min_stars: Option<u8>,
    #[serde(default)]
    pub custom_strategy: Option<String>,
    #[serde(default)]
    pub strategy_stats: HashMap<String, StrategyPerfStats>,
    pub open_positions: Vec<PaperPosition>,
}

#[derive(Debug, Clone)]
pub struct CsvTradeRow {
    pub timestamp: String,
    pub trade_id: String,
    pub event: String,
    pub symbol: String,
    pub side: String,
    pub exec_price: f64,
    pub net_pnl: f64,
    pub strategy: String,
}

impl PaperPortfolioState {
    pub fn new(initial_balance: f64) -> Self {
        Self {
            initial_balance,
            free_balance: initial_balance,
            realized_pnl: 0.0,
            total_fees: 0.0,
            peak_equity: initial_balance,
            max_drawdown_pct: 0.0,
            next_trade_id: 1,
            total_closed: 0,
            win_count: 0,
            loss_count: 0,
            paused_opening: false,
            custom_leverage: None,
            custom_position_pct: None,
            custom_max_open_positions: None,
            custom_min_stars: None,
            custom_strategy: None,
            strategy_stats: HashMap::new(),
            open_positions: Vec::new(),
        }
    }

    pub fn effective_strategy(&self, cfg: &ScannerConfig) -> TradingStrategy {
        self.custom_strategy
            .as_deref()
            .and_then(TradingStrategy::from_str_lossy)
            .unwrap_or(cfg.scanner_strategy)
    }

    pub fn effective_leverage(&self, cfg: &ScannerConfig) -> f64 {
        self.custom_leverage
            .unwrap_or(cfg.paper_leverage)
            .clamp(1.0, 125.0)
    }

    pub fn effective_position_pct(&self, cfg: &ScannerConfig) -> f64 {
        self.custom_position_pct
            .unwrap_or(cfg.paper_position_pct)
            .clamp(1.0, 100.0)
    }

    pub fn effective_max_open_positions(&self, cfg: &ScannerConfig) -> usize {
        self.custom_max_open_positions
            .unwrap_or(cfg.paper_max_open_positions)
            .clamp(1, 30)
    }

    pub fn effective_min_stars(&self, cfg: &ScannerConfig) -> u8 {
        if let Some(s) = self.custom_min_stars {
            return s.clamp(1, 3);
        }
        self.effective_strategy(cfg)
            .default_min_stars(cfg.paper_min_stars)
    }

    pub fn has_custom_settings(&self) -> bool {
        self.custom_leverage.is_some()
            || self.custom_position_pct.is_some()
            || self.custom_max_open_positions.is_some()
            || self.custom_min_stars.is_some()
            || self.custom_strategy.is_some()
    }

    pub fn reset_preserving_settings(&self, initial_balance: f64) -> Self {
        let mut fresh = Self::new(initial_balance);
        fresh.custom_leverage = self.custom_leverage;
        fresh.custom_position_pct = self.custom_position_pct;
        fresh.custom_max_open_positions = self.custom_max_open_positions;
        fresh.custom_min_stars = self.custom_min_stars;
        fresh.custom_strategy = self.custom_strategy.clone();
        fresh
    }

    pub fn locked_margin(&self) -> f64 {
        self.open_positions
            .iter()
            .map(|p| p.remaining_margin)
            .fold(0.0, |acc, x| acc + x)
    }

    pub fn unrealized_pnl(&self) -> f64 {
        self.open_positions
            .iter()
            .map(|p| p.unrealized_pnl())
            .fold(0.0, |acc, x| acc + x)
    }

    pub fn total_equity(&self) -> f64 {
        self.free_balance + self.locked_margin() + self.unrealized_pnl()
    }

    pub fn win_rate_pct(&self) -> f64 {
        if self.total_closed == 0 {
            0.0
        } else {
            (self.win_count as f64 / self.total_closed as f64) * 100.0
        }
    }

    pub fn update_drawdown(&mut self) {
        let eq = self.total_equity();
        if eq > self.peak_equity {
            self.peak_equity = eq;
        }
        if self.peak_equity > 0.0 {
            let dd = ((self.peak_equity - eq) / self.peak_equity) * 100.0;
            if dd > self.max_drawdown_pct {
                self.max_drawdown_pct = dd;
            }
        }
    }
}

#[derive(Debug, Clone)]
struct TradeEventNotification {
    title: String,
    symbol: String,
    lines_th: Vec<String>,
    discord_color: u32,
}

#[derive(Clone)]
pub struct PaperTrader {
    config: ScannerConfig,
    notifier: AlertNotifier,
    state: Arc<Mutex<PaperPortfolioState>>,
}

impl PaperTrader {
    pub fn new(config: ScannerConfig, notifier: AlertNotifier) -> Self {
        let state = Self::load_or_init_sync(&config);
        Self {
            config,
            notifier,
            state: Arc::new(Mutex::new(state)),
        }
    }

    fn load_or_init_sync(config: &ScannerConfig) -> PaperPortfolioState {
        let path = Path::new(&config.paper_state_file);
        if path.exists() {
            if let Ok(raw) = fs::read_to_string(path) {
                if let Ok(parsed) = serde_json::from_str::<PaperPortfolioState>(&raw) {
                    return parsed;
                }
            }
        }
        PaperPortfolioState::new(config.paper_initial_balance)
    }

    pub fn reset_portfolio(&self) -> Result<PaperPortfolioState> {
        let current = Self::load_or_init_sync(&self.config);
        let fresh = current.reset_preserving_settings(self.config.paper_initial_balance);
        if let Ok(mut guard) = self.state.try_lock() {
            *guard = fresh.clone();
        }
        self.save_state_sync(&fresh)?;
        if Path::new(&self.config.paper_trades_csv).exists() {
            fs::remove_file(&self.config.paper_trades_csv)?;
        }
        Ok(fresh)
    }

    pub async fn reset_portfolio_async(&self) -> Result<PaperPortfolioState> {
        let mut st = self.state.lock().await;
        let fresh = st.reset_preserving_settings(self.config.paper_initial_balance);
        *st = fresh.clone();
        self.save_state_sync(&st)?;
        if Path::new(&self.config.paper_trades_csv).exists() {
            let _ = fs::remove_file(&self.config.paper_trades_csv);
        }
        Ok(fresh)
    }

    pub async fn get_state_snapshot(&self) -> PaperPortfolioState {
        self.state.lock().await.clone()
    }

    pub async fn get_effective_strategy_and_min_stars(&self) -> (TradingStrategy, u8) {
        let st = self.state.lock().await;
        (
            st.effective_strategy(&self.config),
            st.effective_min_stars(&self.config),
        )
    }

    pub async fn set_paused(&self, paused: bool) -> PaperPortfolioState {
        let mut st = self.state.lock().await;
        st.paused_opening = paused;
        let _ = self.save_state_sync(&st);
        st.clone()
    }

    pub async fn set_strategy(&self, strategy: TradingStrategy) -> PaperPortfolioState {
        let mut st = self.state.lock().await;
        st.custom_strategy = Some(strategy.key().to_string());
        // Switching strategy automatically resets custom_min_stars so the strategy's own Min Stars takes effect
        // while Leverage, Position %, and Max Open Positions remain untouched.
        st.custom_min_stars = None;
        let _ = self.save_state_sync(&st);
        st.clone()
    }

    pub async fn update_runtime_settings(
        &self,
        max_open_positions: Option<usize>,
        position_pct: Option<f64>,
        leverage: Option<f64>,
        min_stars: Option<u8>,
    ) -> PaperPortfolioState {
        let mut st = self.state.lock().await;
        if let Some(m) = max_open_positions {
            st.custom_max_open_positions = Some(m.clamp(1, 30));
        }
        if let Some(p) = position_pct {
            if p.is_finite() {
                st.custom_position_pct = Some(p.clamp(1.0, 100.0));
            }
        }
        if let Some(l) = leverage {
            if l.is_finite() {
                st.custom_leverage = Some(l.clamp(1.0, 125.0));
            }
        }
        if let Some(s) = min_stars {
            st.custom_min_stars = Some(s.clamp(1, 3));
        }
        let _ = self.save_state_sync(&st);
        st.clone()
    }

    pub async fn clear_runtime_settings(&self) -> PaperPortfolioState {
        let mut st = self.state.lock().await;
        st.custom_leverage = None;
        st.custom_position_pct = None;
        st.custom_max_open_positions = None;
        st.custom_min_stars = None;
        st.custom_strategy = None;
        let _ = self.save_state_sync(&st);
        st.clone()
    }

    pub fn recent_csv_events(&self, limit: usize) -> Vec<CsvTradeRow> {
        let path = Path::new(&self.config.paper_trades_csv);
        if !path.exists() {
            return Vec::new();
        }
        let Ok(content) = fs::read_to_string(path) else {
            return Vec::new();
        };
        let mut rows = Vec::new();
        for line in content.lines().skip(1) {
            let cols: Vec<&str> = line.split(',').collect();
            if cols.len() < 16 {
                continue;
            }
            let strategy = cols.get(19).map(|s| s.trim()).unwrap_or("").to_string();
            rows.push(CsvTradeRow {
                timestamp: cols[0].to_string(),
                trade_id: cols[1].to_string(),
                event: cols[2].to_string(),
                symbol: cols[3].to_string(),
                side: cols[4].to_string(),
                exec_price: cols[9].parse::<f64>().unwrap_or(0.0),
                net_pnl: cols[15].parse::<f64>().unwrap_or(0.0),
                strategy,
            });
        }
        if rows.len() > limit {
            rows = rows.split_off(rows.len() - limit);
        }
        rows.reverse();
        rows
    }

    fn save_state_sync(&self, state: &PaperPortfolioState) -> Result<()> {
        let json = serde_json::to_string_pretty(state)?;
        fs::write(&self.config.paper_state_file, json)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn append_csv_row(
        &self,
        trade_id: u64,
        event: &str,
        pos: &PaperPosition,
        exec_price: f64,
        closed_pct: f64,
        gross_pnl: f64,
        fee: f64,
        net_pnl: f64,
        free_balance_after: f64,
        equity_after: f64,
        note: &str,
    ) {
        let csv_path = Path::new(&self.config.paper_trades_csv);
        let needs_header = !csv_path.exists()
            || fs::metadata(csv_path)
                .map(|m| m.len() == 0)
                .unwrap_or(true);

        let mut file = match OpenOptions::new().create(true).append(true).open(csv_path) {
            Ok(f) => f,
            Err(err) => {
                warn!("ไม่สามารถเปิดไฟล์ CSV {}: {:#}", self.config.paper_trades_csv, err);
                return;
            }
        };

        if needs_header {
            let _ = file.write_all(CSV_HEADER.as_bytes());
        }

        let ts = Utc::now().to_rfc3339();
        let safe_note = note.replace(',', ";");
        let strat_col = if pos.strategy.is_empty() {
            TradingStrategy::Balanced.key()
        } else {
            pos.strategy.as_str()
        };
        let row = format!(
            "{},{},{},{},{},{},{},{},{:.8},{:.8},{:.1},{:.4},{:.1},{:.4},{:.4},{:.4},{:.4},{:.4},{},{}\n",
            ts,
            trade_id,
            event,
            pos.symbol,
            pos.side.as_str(),
            pos.stars,
            pos.timeframe,
            pos.signal_category,
            pos.entry_price,
            exec_price,
            closed_pct,
            pos.initial_margin,
            pos.leverage,
            gross_pnl,
            fee,
            net_pnl,
            free_balance_after,
            equity_after,
            safe_note,
            strat_col
        );
        let _ = file.write_all(row.as_bytes());
    }

    /// Attempts to open a new simulated position when a qualified MTF signal fires.
    pub async fn try_open_position(
        &self,
        symbol: &str,
        direction: SignalDirection,
        category: SignalCategory,
        timeframe: &str,
        stars: u8,
        levels: &MtfTradeLevels,
    ) -> Option<u64> {
        if !self.config.paper_trading_enabled {
            return None;
        }
        let side = match direction {
            SignalDirection::Bullish => PaperSide::Long,
            SignalDirection::Bearish => PaperSide::Short,
            SignalDirection::Neutral => return None,
        };
        if levels.entry_price <= 0.0 {
            return None;
        }

        let notif = {
            let mut st = self.state.lock().await;
            if st.paused_opening {
                return None;
            }
            let active_strat = st.effective_strategy(&self.config);
            let prof = active_strat.trade_profile();
            let eff_min_stars = st.effective_min_stars(&self.config);
            if stars < eff_min_stars {
                return None;
            }
            let eff_max_pos = st.effective_max_open_positions(&self.config);
            if st.open_positions.len() >= eff_max_pos {
                return None;
            }
            if st.open_positions.iter().any(|p| p.symbol == symbol) {
                return None;
            }

            let eff_pos_pct = st.effective_position_pct(&self.config);
            let eff_leverage = st.effective_leverage(&self.config);

            let equity = st.total_equity();
            let desired_margin = equity * (eff_pos_pct / 100.0);
            let fee_rate = self.config.paper_taker_fee_pct / 100.0;
            // Ensure free_balance covers margin + entry_fee
            let max_affordable_margin =
                st.free_balance / (1.0 + eff_leverage * fee_rate);
            let margin = desired_margin.min(max_affordable_margin);

            if margin < 5.0 {
                return None;
            }

            let notional = margin * eff_leverage;
            let qty = notional / levels.entry_price;
            let entry_fee = notional * fee_rate;

            st.free_balance -= margin + entry_fee;
            st.realized_pnl -= entry_fee;
            st.total_fees += entry_fee;

            let trade_id = st.next_trade_id;
            st.next_trade_id += 1;

            let pos = PaperPosition {
                id: trade_id,
                symbol: symbol.to_string(),
                side,
                stars,
                signal_category: category.as_key().to_string(),
                timeframe: timeframe.to_string(),
                strategy: active_strat.key().to_string(),
                tp1_close_pct: prof.tp1_close_pct,
                tp2_close_pct: prof.tp2_close_pct,
                entry_price: levels.entry_price,
                initial_sl: levels.sl,
                current_sl: levels.sl,
                tp1: levels.tp1,
                tp2: levels.tp2,
                tp3: levels.tp3,
                leverage: eff_leverage,
                initial_margin: margin,
                remaining_margin: margin,
                initial_qty: qty,
                remaining_qty: qty,
                remaining_fraction: 1.0,
                tp1_hit: false,
                tp2_hit: false,
                realized_pnl_trade: -entry_fee,
                fees_paid_trade: entry_fee,
                last_price: levels.entry_price,
                opened_at: Utc::now().to_rfc3339(),
            };

            st.open_positions.push(pos.clone());
            st.update_drawdown();
            let eq_after = st.total_equity();
            let free_after = st.free_balance;

            self.append_csv_row(
                trade_id,
                "OPEN",
                &pos,
                levels.entry_price,
                0.0,
                0.0,
                entry_fee,
                -entry_fee,
                free_after,
                eq_after,
                &format!("{} ({}* | {})", category.as_key(), stars, active_strat.short_tag()),
            );
            let _ = self.save_state_sync(&st);

            let (tp1_pct, tp2_pct, tp3_pct) = pos.tp_close_splits();
            let side_emoji = if side == PaperSide::Long { "🟢 LONG" } else { "🔴 SHORT" };
            let title = format!("🧪 [PAPER] เปิดไม้ #{} {} {}", trade_id, side_emoji, symbol);
            let lines_th = vec![
                format!(
                    "🎯 กลยุทธ์: {} | ⭐ {} ({} ดาว | TF: {})",
                    active_strat.title_th(),
                    category.as_key(),
                    stars,
                    timeframe
                ),
                format!(
                    "💰 Margin: ${:.2} ({:.0}x | มูลค่าสัญญา ${:.2})",
                    margin, eff_leverage, notional
                ),
                format!("🎯 ราคาเข้า (Entry): ${}", format_price(levels.entry_price)),
                format!(
                    "📊 เป้าปิด 3 ส่วน: TP1 ${} ({:.0}%) | TP2 ${} ({:.0}%) | TP3 ${} ({:.0}%)",
                    format_price(levels.tp1),
                    tp1_pct,
                    format_price(levels.tp2),
                    tp2_pct,
                    format_price(levels.tp3),
                    tp3_pct
                ),
                format!(
                    "🛡️ Stop Loss: ${} (-{:.2}%) | ค่าธรรมเนียม: ${:.2}",
                    format_price(levels.sl),
                    levels.risk_pct,
                    entry_fee
                ),
                format!(
                    "💼 พอร์ตปัจจุบัน: Equity ${:.2} | ถืออยู่ {}/{} ไม้",
                    eq_after,
                    st.open_positions.len(),
                    eff_max_pos
                ),
            ];
            let color = if side == PaperSide::Long { 0x00E676 } else { 0xFF5252 };

            (
                trade_id,
                TradeEventNotification {
                    title,
                    symbol: symbol.to_string(),
                    lines_th,
                    discord_color: color,
                },
            )
        };

        info!("{} -> {}", notif.1.title, notif.1.lines_th.join(" | "));
        self.notifier
            .send_paper_trade_alert(
                &notif.1.title,
                &notif.1.symbol,
                &notif.1.lines_th,
                notif.1.discord_color,
            )
            .await;

        Some(notif.0)
    }

    /// Updates open positions for a single symbol on every real-time price tick.
    pub async fn on_price_tick(&self, symbol: &str, price: f64) {
        if !self.config.paper_trading_enabled || price <= 0.0 {
            return;
        }
        let mut map = HashMap::with_capacity(1);
        map.insert(symbol.to_string(), price);
        self.update_prices(&map).await;
    }

    /// Evaluates per-strategy partial TP1 (+BE), TP2 (+Trail to TP1), TP3, and SL across open positions.
    pub async fn update_prices(&self, prices: &HashMap<String, f64>) {
        if !self.config.paper_trading_enabled || prices.is_empty() {
            return;
        }

        let notifications: Vec<TradeEventNotification> = {
            let mut st = self.state.lock().await;
            if st.open_positions.is_empty() {
                return;
            }

            let fee_rate = self.config.paper_taker_fee_pct / 100.0;
            let mut events = Vec::new();
            let mut state_changed = false;
            let mut idx = 0;

            while idx < st.open_positions.len() {
                let sym = st.open_positions[idx].symbol.clone();
                let Some(&price) = prices.get(&sym) else {
                    idx += 1;
                    continue;
                };
                if price <= 0.0 {
                    idx += 1;
                    continue;
                }

                st.open_positions[idx].last_price = price;
                let mut pos = st.open_positions[idx].clone();
                let (tp1_pct, tp2_pct, tp3_pct) = pos.tp_close_splits();
                let pos_strat = TradingStrategy::from_str_lossy(&pos.strategy).unwrap_or_default();
                let mut closed_completely = false;

                // 1. Check Stop Loss first if price crossed current_sl
                let sl_hit = match pos.side {
                    PaperSide::Long => price <= pos.current_sl,
                    PaperSide::Short => price >= pos.current_sl,
                };

                if sl_hit {
                    let close_qty = pos.remaining_qty;
                    let exec_price = price;
                    let gross = match pos.side {
                        PaperSide::Long => (exec_price - pos.entry_price) * close_qty,
                        PaperSide::Short => (pos.entry_price - exec_price) * close_qty,
                    };
                    let exit_fee = (close_qty * exec_price) * fee_rate;
                    let raw_net = gross - exit_fee;
                    // Isolated margin cap: cannot lose more than remaining_margin
                    let returned_cash = (pos.remaining_margin + raw_net).max(0.0);
                    let net_step = returned_cash - pos.remaining_margin;

                    pos.realized_pnl_trade += net_step;
                    pos.fees_paid_trade += exit_fee;
                    pos.remaining_qty = 0.0;
                    pos.remaining_margin = 0.0;
                    pos.remaining_fraction = 0.0;

                    st.free_balance += returned_cash;
                    st.realized_pnl += net_step;
                    st.total_fees += exit_fee;
                    st.total_closed += 1;
                    if pos.realized_pnl_trade >= 0.0 {
                        st.win_count += 1;
                    } else {
                        st.loss_count += 1;
                    }
                    record_strategy_closed_trade(
                        &mut st.strategy_stats,
                        &pos.strategy,
                        pos.realized_pnl_trade,
                    );

                    st.open_positions.remove(idx);
                    st.update_drawdown();
                    let eq_after = st.total_equity();
                    let free_after = st.free_balance;

                    let close_reason = if pos.tp2_hit {
                        "TRAILING_SL_TP1"
                    } else if pos.tp1_hit {
                        "BREAKEVEN_SL"
                    } else {
                        "STOP_LOSS"
                    };

                    let pct_closed = (close_qty / pos.initial_qty) * 100.0;
                    self.append_csv_row(
                        pos.id,
                        close_reason,
                        &pos,
                        exec_price,
                        pct_closed,
                        gross,
                        exit_fee,
                        net_step,
                        free_after,
                        eq_after,
                        close_reason,
                    );

                    let icon = if pos.realized_pnl_trade >= 0.0 { "✅" } else { "🛑" };
                    let label = match close_reason {
                        "TRAILING_SL_TP1" => "Trailing SL @ TP1 (ล็อกกำไรสำเร็จ)",
                        "BREAKEVEN_SL" => "Breakeven SL (กันทุนหลังชน TP1)",
                        _ => "Stop Loss (ตัดขาดทุนตามแผน)",
                    };
                    let title = format!("{} [PAPER] ปิดไม้ #{} {} — {}", icon, pos.id, pos.symbol, label);
                    let lines_th = vec![
                        format!(
                            "📌 สถานะ: {} ({}) | 🎯 {}",
                            pos.side.as_str(),
                            label,
                            pos_strat.title_th()
                        ),
                        format!(
                            "🎯 Entry: ${} -> Exit: ${}",
                            format_price(pos.entry_price),
                            format_price(exec_price)
                        ),
                        format!(
                            "💵 กำไร/ขาดทุนสุทธิไม้นี้: {:+.2} USDT (ค่าธรรมเนียมรวม ${:.2})",
                            pos.realized_pnl_trade, pos.fees_paid_trade
                        ),
                        format!(
                            "📊 สถิติพอร์ต: Equity ${:.2} | Win Rate {:.1}% ({}W/{}L) | Max DD {:.2}%",
                            eq_after,
                            st.win_rate_pct(),
                            st.win_count,
                            st.loss_count,
                            st.max_drawdown_pct
                        ),
                    ];
                    events.push(TradeEventNotification {
                        title,
                        symbol: pos.symbol.clone(),
                        lines_th,
                        discord_color: if pos.realized_pnl_trade >= 0.0 { 0x00E676 } else { 0xFF5252 },
                    });

                    state_changed = true;
                    continue;
                }

                // 2. Check TP1 (tp1_pct% + move SL to Breakeven)
                let tp1_hit_now = !pos.tp1_hit
                    && match pos.side {
                        PaperSide::Long => price >= pos.tp1,
                        PaperSide::Short => price <= pos.tp1,
                    };

                if tp1_hit_now {
                    let frac = tp1_pct / 100.0;
                    let remain_after_tp1 = (100.0 - tp1_pct).max(0.0);
                    let close_qty = pos.initial_qty * frac;
                    let margin_released = pos.initial_margin * frac;
                    let exec_price = pos.tp1;
                    let gross = match pos.side {
                        PaperSide::Long => (exec_price - pos.entry_price) * close_qty,
                        PaperSide::Short => (pos.entry_price - exec_price) * close_qty,
                    };
                    let exit_fee = (close_qty * exec_price) * fee_rate;
                    let net_step = gross - exit_fee;

                    pos.tp1_hit = true;
                    pos.remaining_qty = (pos.remaining_qty - close_qty).max(0.0);
                    pos.remaining_margin = (pos.remaining_margin - margin_released).max(0.0);
                    pos.remaining_fraction = (pos.remaining_fraction - frac).max(0.0);
                    pos.realized_pnl_trade += net_step;
                    pos.fees_paid_trade += exit_fee;

                    // Move SL to Breakeven (including entry+exit fee buffer)
                    let be_buffer = fee_rate * 2.2;
                    pos.current_sl = match pos.side {
                        PaperSide::Long => pos.entry_price * (1.0 + be_buffer),
                        PaperSide::Short => pos.entry_price * (1.0 - be_buffer),
                    };

                    st.free_balance += margin_released + net_step;
                    st.realized_pnl += net_step;
                    st.total_fees += exit_fee;
                    st.open_positions[idx] = pos.clone();
                    st.update_drawdown();
                    let eq_after = st.total_equity();
                    let free_after = st.free_balance;

                    self.append_csv_row(
                        pos.id,
                        "TP1_PARTIAL",
                        &pos,
                        exec_price,
                        tp1_pct,
                        gross,
                        exit_fee,
                        net_step,
                        free_after,
                        eq_after,
                        &format!("Closed {:.0}% & Moved SL to Breakeven", tp1_pct),
                    );

                    events.push(TradeEventNotification {
                        title: format!("💰 [PAPER] TP1 แตก! #{} {} (+ปิด {:.0}%)", pos.id, pos.symbol, tp1_pct),
                        symbol: pos.symbol.clone(),
                        lines_th: vec![
                            format!(
                                "🎯 ชนเป้า TP1 ที่ ${} (เก็บกำไร {:.0}% = {:+.2} USDT) [{}]",
                                format_price(exec_price),
                                tp1_pct,
                                net_step,
                                pos_strat.short_tag()
                            ),
                            format!(
                                "🛡️ เลื่อน SL กันทุน (Breakeven) อัตโนมัติ -> ${}",
                                format_price(pos.current_sl)
                            ),
                            format!(
                                "💼 เหลือถือรันต่อ {:.0}% ลุ้น TP2 (${}) / TP3 (${}) | Equity ${:.2}",
                                remain_after_tp1,
                                format_price(pos.tp2),
                                format_price(pos.tp3),
                                eq_after
                            ),
                        ],
                        discord_color: 0x00E676,
                    });
                    state_changed = true;
                }

                // 3. Check TP2 (tp2_pct% + trail SL to TP1)
                let tp2_hit_now = pos.tp1_hit
                    && !pos.tp2_hit
                    && match pos.side {
                        PaperSide::Long => price >= pos.tp2,
                        PaperSide::Short => price <= pos.tp2,
                    };

                if tp2_hit_now {
                    let frac = tp2_pct / 100.0;
                    let close_qty = pos.initial_qty * frac;
                    let margin_released = pos.initial_margin * frac;
                    let exec_price = pos.tp2;
                    let gross = match pos.side {
                        PaperSide::Long => (exec_price - pos.entry_price) * close_qty,
                        PaperSide::Short => (pos.entry_price - exec_price) * close_qty,
                    };
                    let exit_fee = (close_qty * exec_price) * fee_rate;
                    let net_step = gross - exit_fee;

                    pos.tp2_hit = true;
                    pos.remaining_qty = (pos.remaining_qty - close_qty).max(0.0);
                    pos.remaining_margin = (pos.remaining_margin - margin_released).max(0.0);
                    pos.remaining_fraction = (pos.remaining_fraction - frac).max(0.0);
                    pos.realized_pnl_trade += net_step;
                    pos.fees_paid_trade += exit_fee;
                    pos.current_sl = pos.tp1;

                    st.free_balance += margin_released + net_step;
                    st.realized_pnl += net_step;
                    st.total_fees += exit_fee;
                    st.open_positions[idx] = pos.clone();
                    st.update_drawdown();
                    let eq_after = st.total_equity();
                    let free_after = st.free_balance;

                    self.append_csv_row(
                        pos.id,
                        "TP2_PARTIAL",
                        &pos,
                        exec_price,
                        tp2_pct,
                        gross,
                        exit_fee,
                        net_step,
                        free_after,
                        eq_after,
                        &format!("Closed {:.0}% & Trailed SL to TP1", tp2_pct),
                    );

                    events.push(TradeEventNotification {
                        title: format!("🚀 [PAPER] TP2 แตก! #{} {} (+ปิดอีก {:.0}%)", pos.id, pos.symbol, tp2_pct),
                        symbol: pos.symbol.clone(),
                        lines_th: vec![
                            format!(
                                "🎯 ชนเป้า TP2 ที่ ${} (เก็บกำไรเพิ่ม {:.0}% = {:+.2} USDT) [{}]",
                                format_price(exec_price),
                                tp2_pct,
                                net_step,
                                pos_strat.short_tag()
                            ),
                            format!(
                                "🔒 เลื่อน Trailing SL ล็อกกำไรที่ TP1 -> ${}",
                                format_price(pos.current_sl)
                            ),
                            format!(
                                "💼 เหลือถือรันต่อ {:.0}% ลุ้น TP3 (${}) | Equity ${:.2}",
                                tp3_pct,
                                format_price(pos.tp3),
                                eq_after
                            ),
                        ],
                        discord_color: 0x00E676,
                    });
                    state_changed = true;
                }

                // 4. Check TP3 (final remaining fraction / tp3_pct%)
                let tp3_hit_now = pos.tp2_hit
                    && match pos.side {
                        PaperSide::Long => price >= pos.tp3,
                        PaperSide::Short => price <= pos.tp3,
                    };

                if tp3_hit_now {
                    let close_qty = pos.remaining_qty;
                    let margin_released = pos.remaining_margin;
                    let exec_price = pos.tp3;
                    let gross = match pos.side {
                        PaperSide::Long => (exec_price - pos.entry_price) * close_qty,
                        PaperSide::Short => (pos.entry_price - exec_price) * close_qty,
                    };
                    let exit_fee = (close_qty * exec_price) * fee_rate;
                    let net_step = gross - exit_fee;

                    pos.realized_pnl_trade += net_step;
                    pos.fees_paid_trade += exit_fee;
                    pos.remaining_qty = 0.0;
                    pos.remaining_margin = 0.0;
                    pos.remaining_fraction = 0.0;

                    st.free_balance += margin_released + net_step;
                    st.realized_pnl += net_step;
                    st.total_fees += exit_fee;
                    st.total_closed += 1;
                    if pos.realized_pnl_trade >= 0.0 {
                        st.win_count += 1;
                    } else {
                        st.loss_count += 1;
                    }
                    record_strategy_closed_trade(
                        &mut st.strategy_stats,
                        &pos.strategy,
                        pos.realized_pnl_trade,
                    );

                    st.open_positions.remove(idx);
                    st.update_drawdown();
                    let eq_after = st.total_equity();
                    let free_after = st.free_balance;

                    self.append_csv_row(
                        pos.id,
                        "TP3_FULL_CLOSE",
                        &pos,
                        exec_price,
                        tp3_pct,
                        gross,
                        exit_fee,
                        net_step,
                        free_after,
                        eq_after,
                        "Hit TP3 Full Take Profit",
                    );

                    events.push(TradeEventNotification {
                        title: format!("🏆 [PAPER] ปิดกำไรครบ 3 เป้า! #{} {}", pos.id, pos.symbol),
                        symbol: pos.symbol.clone(),
                        lines_th: vec![
                            format!(
                                "🎯 ชนเป้า TP3 ที่ ${} (ปิด {:.0}% สุดท้าย = {:+.2} USDT) [{}]",
                                format_price(exec_price),
                                tp3_pct,
                                net_step,
                                pos_strat.title_th()
                            ),
                            format!(
                                "💎 กำไรรวมสุทธิทั้งไม้: {:+.2} USDT (ค่าธรรมเนียมรวม ${:.2})",
                                pos.realized_pnl_trade, pos.fees_paid_trade
                            ),
                            format!(
                                "📊 สถิติพอร์ต: Equity ${:.2} | Win Rate {:.1}% ({}W/{}L) | Max DD {:.2}%",
                                eq_after,
                                st.win_rate_pct(),
                                st.win_count,
                                st.loss_count,
                                st.max_drawdown_pct
                            ),
                        ],
                        discord_color: 0x00E676,
                    });

                    closed_completely = true;
                    state_changed = true;
                }

                if !closed_completely {
                    idx += 1;
                }
            }

            if state_changed {
                let _ = self.save_state_sync(&st);
            } else {
                st.update_drawdown();
                let _ = self.save_state_sync(&st);
            }

            events
        };

        for ev in notifications {
            info!("{} -> {}", ev.title, ev.lines_th.join(" | "));
            self.notifier
                .send_paper_trade_alert(&ev.title, &ev.symbol, &ev.lines_th, ev.discord_color)
                .await;
        }
    }

    /// Manually closes an open position by trade ID (triggered from Telegram controls).
    pub async fn close_position_by_id(
        &self,
        trade_id: u64,
        market_prices: Option<&HashMap<String, f64>>,
    ) -> Option<(String, f64)> {
        let (ev, result) = {
            let mut st = self.state.lock().await;
            let pos_idx = st.open_positions.iter().position(|p| p.id == trade_id)?;
            let mut pos = st.open_positions.remove(pos_idx);

            let exec_price = market_prices
                .and_then(|m| m.get(&pos.symbol).copied())
                .filter(|&p| p > 0.0)
                .unwrap_or(pos.last_price);
            pos.last_price = exec_price;

            let fee_rate = self.config.paper_taker_fee_pct / 100.0;
            let close_qty = pos.remaining_qty;
            let gross = match pos.side {
                PaperSide::Long => (exec_price - pos.entry_price) * close_qty,
                PaperSide::Short => (pos.entry_price - exec_price) * close_qty,
            };
            let exit_fee = (close_qty * exec_price) * fee_rate;
            let raw_net = gross - exit_fee;
            let returned_cash = (pos.remaining_margin + raw_net).max(0.0);
            let net_step = returned_cash - pos.remaining_margin;

            pos.realized_pnl_trade += net_step;
            pos.fees_paid_trade += exit_fee;
            pos.remaining_qty = 0.0;
            pos.remaining_margin = 0.0;
            pos.remaining_fraction = 0.0;

            st.free_balance += returned_cash;
            st.realized_pnl += net_step;
            st.total_fees += exit_fee;
            st.total_closed += 1;
            if pos.realized_pnl_trade >= 0.0 {
                st.win_count += 1;
            } else {
                st.loss_count += 1;
            }
            record_strategy_closed_trade(
                &mut st.strategy_stats,
                &pos.strategy,
                pos.realized_pnl_trade,
            );

            st.update_drawdown();
            let eq_after = st.total_equity();
            let free_after = st.free_balance;
            let pct_closed = if pos.initial_qty > 0.0 {
                (close_qty / pos.initial_qty) * 100.0
            } else {
                100.0
            };

            self.append_csv_row(
                pos.id,
                "MANUAL_CLOSE",
                &pos,
                exec_price,
                pct_closed,
                gross,
                exit_fee,
                net_step,
                free_after,
                eq_after,
                "Closed manually via Telegram",
            );
            let _ = self.save_state_sync(&st);

            let pos_strat = TradingStrategy::from_str_lossy(&pos.strategy).unwrap_or_default();
            let icon = if pos.realized_pnl_trade >= 0.0 { "✅" } else { "🛑" };
            let notif = TradeEventNotification {
                title: format!(
                    "{} [PAPER] ปิดมือ #{} {} — สั่งปิดจาก Telegram",
                    icon, pos.id, pos.symbol
                ),
                symbol: pos.symbol.clone(),
                lines_th: vec![
                    format!(
                        "📌 สถานะ: {} (ปิดด้วยตนเอง) | 🎯 {}",
                        pos.side.as_str(),
                        pos_strat.title_th()
                    ),
                    format!(
                        "🎯 Entry: ${} -> Exit: ${}",
                        format_price(pos.entry_price),
                        format_price(exec_price)
                    ),
                    format!(
                        "💵 กำไร/ขาดทุนสุทธิไม้นี้: {:+.2} USDT (ค่าธรรมเนียมรวม ${:.2})",
                        pos.realized_pnl_trade, pos.fees_paid_trade
                    ),
                    format!(
                        "📊 สถิติพอร์ต: Equity ${:.2} | Win Rate {:.1}% ({}W/{}L)",
                        eq_after,
                        st.win_rate_pct(),
                        st.win_count,
                        st.loss_count
                    ),
                ],
                discord_color: if pos.realized_pnl_trade >= 0.0 {
                    0x00E676
                } else {
                    0xFF5252
                },
            };

            (notif, (pos.symbol, pos.realized_pnl_trade))
        };

        info!("{} -> {}", ev.title, ev.lines_th.join(" | "));
        self.notifier
            .send_paper_trade_alert(&ev.title, &ev.symbol, &ev.lines_th, ev.discord_color)
            .await;
        Some(result)
    }

    /// Manually closes all open positions (triggered from Telegram Close All).
    pub async fn close_all_positions(
        &self,
        market_prices: Option<&HashMap<String, f64>>,
    ) -> (usize, f64) {
        let ids: Vec<u64> = {
            let st = self.state.lock().await;
            st.open_positions.iter().map(|p| p.id).collect()
        };
        let mut closed_count = 0usize;
        let mut sum_pnl = 0.0f64;
        for id in ids {
            if let Some((_, trade_pnl)) = self.close_position_by_id(id, market_prices).await {
                closed_count += 1;
                sum_pnl += trade_pnl;
            }
        }
        (closed_count, sum_pnl)
    }

    pub async fn print_portfolio_summary(&self) {
        let st = self.state.lock().await;
        let eq = st.total_equity();
        let net_pct = if st.initial_balance > 0.0 {
            ((eq - st.initial_balance) / st.initial_balance) * 100.0
        } else {
            0.0
        };

        let eff_strat = st.effective_strategy(&self.config);
        let eff_lev = st.effective_leverage(&self.config);
        let eff_pct = st.effective_position_pct(&self.config);
        let eff_max = st.effective_max_open_positions(&self.config);
        let eff_stars = st.effective_min_stars(&self.config);

        info!("==============================================================");
        info!("💼 สรุปพอร์ตจำลองการเทรด (Paper Trading Portfolio)");
        info!(
            "• Equity รวม: ${:.2} ({:+.2}%) | ทุนเริ่มต้น: ${:.2}",
            eq, net_pct, st.initial_balance
        );
        info!(
            "• กลยุทธ์: {} | ตั้งค่าไม้: {:.0}x | ไม้ละ {:.0}% | Max {} ไม้ | >= {} ดาว{}",
            eff_strat.title_th(),
            eff_lev,
            eff_pct,
            eff_max,
            eff_stars,
            if st.has_custom_settings() { " [ปรับผ่าน Telegram]" } else { "" }
        );
        info!(
            "• เงินว่าง (Free): ${:.2} | Margin ที่ถืออยู่: ${:.2} | Unrealized: {:+.2} USDT",
            st.free_balance,
            st.locked_margin(),
            st.unrealized_pnl()
        );
        info!(
            "• สถิติปิดไม้: {} ไม้ | Win Rate: {:.1}% ({}W / {}L) | Realized PnL: {:+.2} | Max DD: {:.2}%",
            st.total_closed,
            st.win_rate_pct(),
            st.win_count,
            st.loss_count,
            st.realized_pnl,
            st.max_drawdown_pct
        );
        if st.open_positions.is_empty() {
            info!("• ออเดอร์ที่ถืออยู่: ไม่มี (รอสัญญาณ >= {} ดาว)", eff_stars);
        } else {
            info!(
                "• ออเดอร์ที่ถืออยู่ ({}/{}):",
                st.open_positions.len(),
                eff_max
            );
            for p in &st.open_positions {
                let remain_pct = (p.remaining_fraction * 100.0).round();
                let pos_strat = TradingStrategy::from_str_lossy(&p.strategy).unwrap_or_default();
                let stage = if p.tp2_hit {
                    format!("ผ่าน TP1+TP2 (ถือ {:.0}%)", remain_pct)
                } else if p.tp1_hit {
                    format!("ผ่าน TP1+กันทุน (ถือ {:.0}%)", remain_pct)
                } else {
                    "รอ TP1 (ถือ 100%)".to_string()
                };
                info!(
                    "  #{} {} {} [{}] | Entry ${} -> Now ${} | uPnL {:+.2} ({:+.1}%) | SL ${} | {}",
                    p.id,
                    p.side.as_str(),
                    p.symbol,
                    pos_strat.short_tag(),
                    format_price(p.entry_price),
                    format_price(p.last_price),
                    p.unrealized_pnl(),
                    p.unrealized_roe_pct(),
                    format_price(p.current_sl),
                    stage
                );
            }
        }
        info!("==============================================================");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_config(tag: &str) -> ScannerConfig {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut cfg = ScannerConfig::from_env();
        cfg.paper_trading_enabled = true;
        cfg.paper_initial_balance = 1000.0;
        cfg.paper_leverage = 10.0;
        cfg.paper_position_pct = 10.0;
        cfg.paper_max_open_positions = 5;
        cfg.paper_min_stars = 2;
        cfg.paper_taker_fee_pct = 0.05;
        cfg.paper_notify_webhooks = false;
        cfg.paper_state_file = format!("/tmp/scanner_f_test_{}_{}.json", tag, nanos);
        cfg.paper_trades_csv = format!("/tmp/scanner_f_test_{}_{}.csv", tag, nanos);
        cfg
    }

    #[tokio::test]
    async fn test_paper_trading_partial_tps_and_tp3_full_win() {
        let cfg = test_config("win");
        let notifier = AlertNotifier::new(cfg.clone());
        let pt = PaperTrader::new(cfg.clone(), notifier);

        let levels = MtfTradeLevels {
            entry_price: 100.0,
            entry_band: 99.5,
            sl: 98.0,
            tp1: 102.0,
            tp2: 104.0,
            tp3: 106.0,
            risk_pct: 2.0,
            tp1_pct: 2.0,
            tp2_pct: 4.0,
            tp3_pct: 6.0,
        };

        let id = pt
            .try_open_position(
                "BTCUSDT",
                SignalDirection::Bullish,
                SignalCategory::RsiOversold,
                "5m+15m+1h",
                3,
                &levels,
            )
            .await
            .unwrap();
        assert_eq!(id, 1);

        // TP1 hit at 102.0 -> closes 40%, moves SL to breakeven (>100.0)
        pt.on_price_tick("BTCUSDT", 102.0).await;
        {
            let st = pt.state.lock().await;
            assert_eq!(st.open_positions.len(), 1);
            assert!(st.open_positions[0].tp1_hit);
            assert!(!st.open_positions[0].tp2_hit);
            assert!(st.open_positions[0].current_sl > 100.0);
            assert!((st.open_positions[0].remaining_fraction - 0.6).abs() < 1e-6);
        }

        // TP2 hit at 104.0 -> closes 30%, trails SL to TP1 (102.0)
        pt.on_price_tick("BTCUSDT", 104.0).await;
        {
            let st = pt.state.lock().await;
            assert!(st.open_positions[0].tp2_hit);
            assert!((st.open_positions[0].current_sl - 102.0).abs() < 1e-6);
            assert!((st.open_positions[0].remaining_fraction - 0.3).abs() < 1e-6);
        }

        // TP3 hit at 106.0 -> closes final 30%
        pt.on_price_tick("BTCUSDT", 106.0).await;
        {
            let st = pt.state.lock().await;
            assert_eq!(st.open_positions.len(), 0);
            assert_eq!(st.total_closed, 1);
            assert_eq!(st.win_count, 1);
            assert!(st.realized_pnl > 30.0);
            assert!(st.free_balance > 1030.0);
        }

        let _ = fs::remove_file(&cfg.paper_state_file);
        let _ = fs::remove_file(&cfg.paper_trades_csv);
    }

    #[tokio::test]
    async fn test_paper_trading_tp1_then_breakeven_sl_is_still_profitable() {
        let cfg = test_config("be");
        let notifier = AlertNotifier::new(cfg.clone());
        let pt = PaperTrader::new(cfg.clone(), notifier);

        let levels = MtfTradeLevels {
            entry_price: 100.0,
            entry_band: 100.5,
            sl: 102.0,
            tp1: 98.0,
            tp2: 96.0,
            tp3: 94.0,
            risk_pct: 2.0,
            tp1_pct: 2.0,
            tp2_pct: 4.0,
            tp3_pct: 6.0,
        };

        pt.try_open_position(
            "ETHUSDT",
            SignalDirection::Bearish,
            SignalCategory::RsiOverbought,
            "5m+15m+1h",
            2,
            &levels,
        )
        .await
        .unwrap();

        // Short hits TP1 at 98.0
        pt.on_price_tick("ETHUSDT", 98.0).await;
        // Price bounces back up past breakeven SL (~99.89)
        pt.on_price_tick("ETHUSDT", 99.95).await;

        {
            let st = pt.state.lock().await;
            assert_eq!(st.open_positions.len(), 0);
            assert_eq!(st.total_closed, 1);
            assert_eq!(st.win_count, 1);
            assert!(st.free_balance > 1005.0);
        }

        let _ = fs::remove_file(&cfg.paper_state_file);
        let _ = fs::remove_file(&cfg.paper_trades_csv);
    }

    #[tokio::test]
    async fn test_paper_trading_pause_and_manual_close() {
        let cfg = test_config("manual");
        let notifier = AlertNotifier::new(cfg.clone());
        let pt = PaperTrader::new(cfg.clone(), notifier);

        let levels = MtfTradeLevels {
            entry_price: 100.0,
            entry_band: 99.5,
            sl: 98.0,
            tp1: 102.0,
            tp2: 104.0,
            tp3: 106.0,
            risk_pct: 2.0,
            tp1_pct: 2.0,
            tp2_pct: 4.0,
            tp3_pct: 6.0,
        };

        // Pause should block new openings
        let st_paused = pt.set_paused(true).await;
        assert!(st_paused.paused_opening);
        assert!(pt
            .try_open_position(
                "SOLUSDT",
                SignalDirection::Bullish,
                SignalCategory::PricePump,
                "5m+15m+1h",
                3,
                &levels
            )
            .await
            .is_none());

        // Resume allows opening again
        pt.set_paused(false).await;
        let tid = pt
            .try_open_position(
                "SOLUSDT",
                SignalDirection::Bullish,
                SignalCategory::PricePump,
                "5m+15m+1h",
                3,
                &levels,
            )
            .await
            .unwrap();

        let mut prices = HashMap::new();
        prices.insert("SOLUSDT".to_string(), 101.0);
        let closed = pt.close_position_by_id(tid, Some(&prices)).await.unwrap();
        assert_eq!(closed.0, "SOLUSDT");
        assert!(closed.1 > 8.0);
        assert_eq!(pt.recent_csv_events(5).len(), 2);

        pt.reset_portfolio_async().await.unwrap();
        let snap = pt.get_state_snapshot().await;
        assert_eq!(snap.total_closed, 0);
        assert!((snap.free_balance - 1000.0).abs() < 1e-6);

        let _ = fs::remove_file(&cfg.paper_state_file);
        let _ = fs::remove_file(&cfg.paper_trades_csv);
    }

    #[tokio::test]
    async fn test_runtime_settings_override_persistence_and_reset() {
        let cfg = test_config("settings");
        let notifier = AlertNotifier::new(cfg.clone());
        let pt = PaperTrader::new(cfg.clone(), notifier);

        let levels = MtfTradeLevels {
            entry_price: 100.0,
            entry_band: 99.5,
            sl: 98.0,
            tp1: 102.0,
            tp2: 104.0,
            tp3: 106.0,
            risk_pct: 2.0,
            tp1_pct: 2.0,
            tp2_pct: 4.0,
            tp3_pct: 6.0,
        };

        // Override max=1, size=20%, lev=20x, min_stars=3
        let st_custom = pt
            .update_runtime_settings(Some(1), Some(20.0), Some(20.0), Some(3))
            .await;
        assert!(st_custom.has_custom_settings());
        assert_eq!(st_custom.effective_max_open_positions(&cfg), 1);
        assert!((st_custom.effective_position_pct(&cfg) - 20.0).abs() < 1e-6);
        assert!((st_custom.effective_leverage(&cfg) - 20.0).abs() < 1e-6);
        assert_eq!(st_custom.effective_min_stars(&cfg), 3);

        // 2-star signal blocked when min_stars=3
        assert!(pt
            .try_open_position(
                "BTCUSDT",
                SignalDirection::Bullish,
                SignalCategory::PricePump,
                "5m+15m+1h",
                2,
                &levels,
            )
            .await
            .is_none());

        // 3-star signal opens using custom 20% ($200 margin) & 20x leverage
        let tid1 = pt
            .try_open_position(
                "BTCUSDT",
                SignalDirection::Bullish,
                SignalCategory::PricePump,
                "5m+15m+1h",
                3,
                &levels,
            )
            .await
            .unwrap();
        assert_eq!(tid1, 1);
        {
            let snap = pt.get_state_snapshot().await;
            assert!((snap.open_positions[0].initial_margin - 200.0).abs() < 1e-6);
            assert!((snap.open_positions[0].leverage - 20.0).abs() < 1e-6);
        }

        // Max=1 should block second symbol
        assert!(pt
            .try_open_position(
                "ETHUSDT",
                SignalDirection::Bullish,
                SignalCategory::PricePump,
                "5m+15m+1h",
                3,
                &levels,
            )
            .await
            .is_none());

        // Reset portfolio preserves custom Telegram settings!
        let st_reset = pt.reset_portfolio_async().await.unwrap();
        assert_eq!(st_reset.open_positions.len(), 0);
        assert!(st_reset.has_custom_settings());
        assert_eq!(st_reset.effective_max_open_positions(&cfg), 1);
        assert!((st_reset.effective_leverage(&cfg) - 20.0).abs() < 1e-6);

        // Re-loading from disk preserves overrides
        let pt_reloaded = PaperTrader::new(cfg.clone(), AlertNotifier::new(cfg.clone()));
        let snap_reloaded = pt_reloaded.get_state_snapshot().await;
        assert_eq!(snap_reloaded.effective_max_open_positions(&cfg), 1);
        assert!((snap_reloaded.effective_position_pct(&cfg) - 20.0).abs() < 1e-6);

        // Clearing overrides returns to .env defaults
        let st_default = pt_reloaded.clear_runtime_settings().await;
        assert!(!st_default.has_custom_settings());
        assert_eq!(st_default.effective_max_open_positions(&cfg), 5);
        assert!((st_default.effective_position_pct(&cfg) - 10.0).abs() < 1e-6);
        assert!((st_default.effective_leverage(&cfg) - 10.0).abs() < 1e-6);
        assert_eq!(st_default.effective_min_stars(&cfg), 2);

        let _ = fs::remove_file(&cfg.paper_state_file);
        let _ = fs::remove_file(&cfg.paper_trades_csv);
    }

    #[tokio::test]
    async fn test_strategy_switching_min_stars_dynamic_splits_and_stats() {
        let cfg = test_config("strategy");
        let notifier = AlertNotifier::new(cfg.clone());
        let pt = PaperTrader::new(cfg.clone(), notifier);

        // Customize Lev/Size/Max first and switch to TrendFollow -> Min Stars becomes 3 while Lev/Size/Max preserved
        pt.update_runtime_settings(Some(8), Some(20.0), Some(15.0), None)
            .await;
        let st_trend = pt.set_strategy(TradingStrategy::TrendFollow).await;
        assert_eq!(st_trend.effective_strategy(&cfg), TradingStrategy::TrendFollow);
        assert_eq!(st_trend.effective_min_stars(&cfg), 3);
        assert_eq!(st_trend.effective_max_open_positions(&cfg), 8);
        assert!((st_trend.effective_position_pct(&cfg) - 20.0).abs() < 1e-6);
        assert!((st_trend.effective_leverage(&cfg) - 15.0).abs() < 1e-6);

        // Manual star override should still work after selecting a strategy
        let st_manual_stars = pt
            .update_runtime_settings(None, None, None, Some(2))
            .await;
        assert_eq!(st_manual_stars.effective_min_stars(&cfg), 2);

        // Switching to ScalpQuick clears manual star override -> ScalpQuick default (2) while keeping Lev/Size/Max
        let st_scalp = pt.set_strategy(TradingStrategy::ScalpQuick).await;
        assert_eq!(st_scalp.effective_strategy(&cfg), TradingStrategy::ScalpQuick);
        assert_eq!(st_scalp.effective_min_stars(&cfg), 2);
        assert_eq!(st_scalp.effective_max_open_positions(&cfg), 8);
        assert!((st_scalp.effective_leverage(&cfg) - 15.0).abs() < 1e-6);

        // Switch back to TrendFollow and open a 3-star position (25% / 35% / 40% splits)
        pt.set_strategy(TradingStrategy::TrendFollow).await;
        let levels = MtfTradeLevels {
            entry_price: 100.0,
            entry_band: 99.5,
            sl: 98.0,
            tp1: 102.0,
            tp2: 104.0,
            tp3: 106.0,
            risk_pct: 2.0,
            tp1_pct: 2.0,
            tp2_pct: 4.0,
            tp3_pct: 6.0,
        };
        assert!(pt
            .try_open_position(
                "BTCUSDT",
                SignalDirection::Bullish,
                SignalCategory::EmaBullishCross,
                "5m+15m+1h",
                2,
                &levels,
            )
            .await
            .is_none());

        let tid_trend = pt
            .try_open_position(
                "BTCUSDT",
                SignalDirection::Bullish,
                SignalCategory::EmaBullishCross,
                "5m+15m+1h",
                3,
                &levels,
            )
            .await
            .unwrap();
        assert_eq!(tid_trend, 1);

        // Switch live to ScalpQuick WHILE BTCUSDT position is open; BTCUSDT must retain 25/35/40 splits
        pt.set_strategy(TradingStrategy::ScalpQuick).await;
        pt.on_price_tick("BTCUSDT", 102.0).await;
        {
            let snap = pt.get_state_snapshot().await;
            assert_eq!(snap.open_positions[0].strategy, "TREND_FOLLOW");
            assert!((snap.open_positions[0].remaining_fraction - 0.75).abs() < 1e-6);
        }
        pt.on_price_tick("BTCUSDT", 104.0).await;
        {
            let snap = pt.get_state_snapshot().await;
            assert!((snap.open_positions[0].remaining_fraction - 0.40).abs() < 1e-6);
        }
        pt.on_price_tick("BTCUSDT", 106.0).await;

        // Open a 2-star ScalpQuick short and stop it out to verify loss tracking in strategy_stats
        let levels_short = MtfTradeLevels {
            entry_price: 100.0,
            entry_band: 100.5,
            sl: 102.0,
            tp1: 98.0,
            tp2: 96.0,
            tp3: 94.0,
            risk_pct: 2.0,
            tp1_pct: 2.0,
            tp2_pct: 4.0,
            tp3_pct: 6.0,
        };
        let tid_scalp = pt
            .try_open_position(
                "ETHUSDT",
                SignalDirection::Bearish,
                SignalCategory::PriceDump,
                "5m+15m+1h",
                2,
                &levels_short,
            )
            .await
            .unwrap();
        assert_eq!(tid_scalp, 2);
        {
            let snap = pt.get_state_snapshot().await;
            assert_eq!(snap.open_positions[0].strategy, "SCALP_QUICK");
            assert!((snap.open_positions[0].tp1_close_pct - 50.0).abs() < 1e-6);
            assert!((snap.open_positions[0].tp2_close_pct - 35.0).abs() < 1e-6);
        }
        pt.on_price_tick("ETHUSDT", 102.0).await;

        let snap_after = pt.get_state_snapshot().await;
        assert_eq!(snap_after.open_positions.len(), 0);
        let trend_stats = snap_after.strategy_stats.get("TREND_FOLLOW").unwrap();
        assert_eq!(trend_stats.trades, 1);
        assert_eq!(trend_stats.wins, 1);
        assert_eq!(trend_stats.losses, 0);
        assert!(trend_stats.realized_pnl > 0.0);

        let scalp_stats = snap_after.strategy_stats.get("SCALP_QUICK").unwrap();
        assert_eq!(scalp_stats.trades, 1);
        assert_eq!(scalp_stats.wins, 0);
        assert_eq!(scalp_stats.losses, 1);
        assert!(scalp_stats.realized_pnl < 0.0);

        let csv_rows = pt.recent_csv_events(10);
        assert!(csv_rows.iter().any(|r| r.strategy == "TREND_FOLLOW"));
        assert!(csv_rows.iter().any(|r| r.strategy == "SCALP_QUICK"));

        // Portfolio reset clears strategy_stats while preserving custom_strategy
        let st_reset = pt.reset_portfolio_async().await.unwrap();
        assert!(st_reset.strategy_stats.is_empty());
        assert_eq!(st_reset.effective_strategy(&cfg), TradingStrategy::ScalpQuick);

        // Legacy JSON without custom_strategy/strategy_stats/position splits deserializes safely
        let legacy_json = r#"{
            "initial_balance": 1000.0,
            "free_balance": 900.0,
            "realized_pnl": 0.0,
            "total_fees": 0.5,
            "peak_equity": 1000.0,
            "max_drawdown_pct": 0.0,
            "next_trade_id": 2,
            "total_closed": 0,
            "win_count": 0,
            "loss_count": 0,
            "open_positions": [{
                "id": 1,
                "symbol": "BTCUSDT",
                "side": "Long",
                "stars": 3,
                "signal_category": "RSI_OS",
                "timeframe": "5m+15m+1h",
                "entry_price": 100.0,
                "initial_sl": 98.0,
                "current_sl": 98.0,
                "tp1": 102.0,
                "tp2": 104.0,
                "tp3": 106.0,
                "leverage": 10.0,
                "initial_margin": 100.0,
                "remaining_margin": 100.0,
                "initial_qty": 10.0,
                "remaining_qty": 10.0,
                "remaining_fraction": 1.0,
                "tp1_hit": false,
                "tp2_hit": false,
                "realized_pnl_trade": -0.5,
                "fees_paid_trade": 0.5,
                "last_price": 100.0,
                "opened_at": "2026-11-16T00:00:00Z"
            }]
        }"#;
        let legacy_state: PaperPortfolioState = serde_json::from_str(legacy_json).unwrap();
        assert_eq!(legacy_state.effective_strategy(&cfg), TradingStrategy::Balanced);
        assert_eq!(legacy_state.open_positions[0].strategy, "BALANCED");
        assert_eq!(legacy_state.open_positions[0].tp_close_splits(), (40.0, 30.0, 30.0));

        let _ = fs::remove_file(&cfg.paper_state_file);
        let _ = fs::remove_file(&cfg.paper_trades_csv);
    }
}
