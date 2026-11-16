use std::env;

#[derive(Debug, Clone)]
pub struct ScannerConfig {
    /// Binance Futures REST base URL
    pub rest_base_url: String,
    /// Binance Futures WebSocket base URL
    pub ws_base_url: String,

    /// Minimum 24h quote volume in USDT to include a symbol in scanning
    pub min_quote_volume_24h: f64,
    /// Maximum number of top USDT-M symbols by volume to track simultaneously
    pub max_symbols: usize,
    /// Kline interval for technical indicators & pump/dump detection (e.g., "1m", "5m", "15m")
    pub kline_interval: String,
    /// Number of historical candles to keep in memory per symbol
    pub candle_buffer_size: usize,

    /// Price Pump/Dump threshold (%) within a single candle
    pub price_change_threshold_pct: f64,
    /// Volume spike multiplier relative to moving average of volume
    pub volume_spike_multiplier: f64,
    /// Volume moving average lookback period
    pub volume_lookback: usize,

    /// RSI settings
    pub rsi_period: usize,
    pub rsi_overbought: f64,
    pub rsi_oversold: f64,

    /// EMA crossover periods
    pub ema_fast_period: usize,
    pub ema_slow_period: usize,

    /// Bollinger Bands settings
    pub bb_period: usize,
    pub bb_std_dev: f64,
    /// Trigger Bollinger breakout alert only when accompanied by relative volume >= this ratio
    pub bb_min_volume_ratio: f64,

    /// Futures-specific thresholds
    /// Absolute funding rate threshold in percent (e.g., 0.10 means +0.10% or -0.10%)
    pub funding_rate_threshold_pct: f64,
    /// Open interest percentage change threshold between polling intervals (e.g., 3.0 means >= +3.0% or <= -3.0%)
    pub oi_spike_threshold_pct: f64,
    /// Polling interval in seconds for Funding Rate & Open Interest
    pub futures_poll_interval_secs: u64,
    /// Minimum single liquidation order value in USD to trigger an alert
    pub min_liquidation_usd: f64,

    /// Alert cooldown per (symbol, signal_category) in seconds
    pub alert_cooldown_secs: u64,

    /// Optional maximum runtime in minutes (useful for GitHub Actions jobs)
    pub run_duration_mins: Option<u64>,

    /// Path to persistent state file (stores previous Open Interest & cooldowns between runs)
    pub state_file: String,

    /// Paper Trading Simulator settings
    pub paper_trading_enabled: bool,
    pub paper_initial_balance: f64,
    pub paper_leverage: f64,
    pub paper_position_pct: f64,
    pub paper_max_open_positions: usize,
    pub paper_min_stars: u8,
    pub paper_taker_fee_pct: f64,
    pub paper_notify_webhooks: bool,
    pub paper_state_file: String,
    pub paper_trades_csv: String,

    /// Notification webhooks / tokens
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub discord_webhook_url: Option<String>,
}

impl ScannerConfig {
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();

        Self {
            rest_base_url: env_string("BINANCE_FAPI_URL", "https://fapi.binance.com")
                .trim_end_matches('/')
                .to_string(),
            ws_base_url: env_string("BINANCE_FSTREAM_URL", "wss://fstream.binance.com")
                .trim_end_matches('/')
                .to_string(),

            min_quote_volume_24h: env_f64("MIN_QUOTE_VOLUME_24H", 20_000_000.0),
            max_symbols: env_usize("MAX_SYMBOLS", 150),
            kline_interval: env_string("KLINE_INTERVAL", "5m"),
            candle_buffer_size: env_usize("CANDLE_BUFFER_SIZE", 100),

            price_change_threshold_pct: env_f64("PRICE_CHANGE_THRESHOLD_PCT", 2.5),
            volume_spike_multiplier: env_f64("VOLUME_SPIKE_MULTIPLIER", 3.5),
            volume_lookback: env_usize("VOLUME_LOOKBACK", 20),

            rsi_period: env_usize("RSI_PERIOD", 14),
            rsi_overbought: env_f64("RSI_OVERBOUGHT", 75.0),
            rsi_oversold: env_f64("RSI_OVERSOLD", 25.0),

            ema_fast_period: env_usize("EMA_FAST_PERIOD", 9),
            ema_slow_period: env_usize("EMA_SLOW_PERIOD", 21),

            bb_period: env_usize("BB_PERIOD", 20),
            bb_std_dev: env_f64("BB_STD_DEV", 2.0),
            bb_min_volume_ratio: env_f64("BB_MIN_VOLUME_RATIO", 1.8),

            funding_rate_threshold_pct: env_f64("FUNDING_RATE_THRESHOLD_PCT", 0.10),
            oi_spike_threshold_pct: env_f64("OI_SPIKE_THRESHOLD_PCT", 3.0),
            futures_poll_interval_secs: env_u64("FUTURES_POLL_INTERVAL_SECS", 60),
            min_liquidation_usd: env_f64("MIN_LIQUIDATION_USD", 50_000.0),

            alert_cooldown_secs: env_u64("ALERT_COOLDOWN_SECS", 900),
            run_duration_mins: env::var("RUN_DURATION_MINS")
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .filter(|&v| v > 0),
            state_file: env_string("SCANNER_STATE_FILE", ".scanner_state.json"),

            paper_trading_enabled: env_bool("PAPER_TRADING_ENABLED", true),
            paper_initial_balance: env_f64("PAPER_INITIAL_BALANCE", 1000.0),
            paper_leverage: env_f64("PAPER_LEVERAGE", 10.0),
            paper_position_pct: env_f64("PAPER_POSITION_PCT", 10.0),
            paper_max_open_positions: env_usize("PAPER_MAX_OPEN_POSITIONS", 5),
            paper_min_stars: (env_usize("PAPER_MIN_STARS", 2).clamp(1, 3)) as u8,
            paper_taker_fee_pct: env_f64("PAPER_TAKER_FEE_PCT", 0.05),
            paper_notify_webhooks: env_bool("PAPER_NOTIFY_WEBHOOKS", true),
            paper_state_file: env_string("PAPER_STATE_FILE", "paper_state.json"),
            paper_trades_csv: env_string("PAPER_TRADES_CSV", "paper_trades.csv"),

            telegram_bot_token: env_opt_string("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id: env_opt_string("TELEGRAM_CHAT_ID"),
            discord_webhook_url: env_opt_string("DISCORD_WEBHOOK_URL"),
        }
    }

    pub fn has_telegram(&self) -> bool {
        self.telegram_bot_token.is_some() && self.telegram_chat_id.is_some()
    }

    pub fn has_discord(&self) -> bool {
        self.discord_webhook_url.is_some()
    }
}

fn env_string(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_opt_string(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn env_f64(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .map(|v| match v.trim().to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        })
        .unwrap_or(default)
}
