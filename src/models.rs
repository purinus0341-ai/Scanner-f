use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeInfoResponse {
    pub symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SymbolInfo {
    pub symbol: String,
    pub contract_type: String,
    pub status: String,
    pub quote_asset: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ticker24hr {
    pub symbol: String,
    pub last_price: String,
    pub price_change_percent: String,
    pub volume: String,
    pub quote_volume: String,
}

impl Ticker24hr {
    pub fn quote_volume_f64(&self) -> f64 {
        self.quote_volume.parse::<f64>().unwrap_or(0.0)
    }

    pub fn last_price_f64(&self) -> f64 {
        self.last_price.parse::<f64>().unwrap_or(0.0)
    }

    pub fn price_change_pct_f64(&self) -> f64 {
        self.price_change_percent.parse::<f64>().unwrap_or(0.0)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PremiumIndex {
    pub symbol: String,
    pub mark_price: String,
    pub index_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: i64,
}

impl PremiumIndex {
    pub fn funding_rate_pct(&self) -> f64 {
        self.last_funding_rate.parse::<f64>().unwrap_or(0.0) * 100.0
    }

    pub fn mark_price_f64(&self) -> f64 {
        self.mark_price.parse::<f64>().unwrap_or(0.0)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenInterestResponse {
    pub symbol: String,
    pub open_interest: String,
    pub time: Option<i64>,
}

impl OpenInterestResponse {
    pub fn open_interest_f64(&self) -> f64 {
        self.open_interest.parse::<f64>().unwrap_or(0.0)
    }
}

/// Combined WebSocket wrapper from `/stream?streams=...`
#[derive(Debug, Clone, Deserialize)]
pub struct CombinedStreamWrapper<T> {
    pub stream: String,
    pub data: T,
}

/// Kline WebSocket event payload (`<symbol>@kline_<interval>`)
#[derive(Debug, Clone, Deserialize)]
pub struct WsKlineEvent {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "k")]
    pub kline: WsKlineData,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WsKlineData {
    #[serde(rename = "t")]
    pub open_time: i64,
    #[serde(rename = "T")]
    pub close_time: i64,
    #[serde(rename = "i")]
    pub interval: String,
    #[serde(rename = "o")]
    pub open: String,
    #[serde(rename = "c")]
    pub close: String,
    #[serde(rename = "h")]
    pub high: String,
    #[serde(rename = "l")]
    pub low: String,
    #[serde(rename = "v")]
    pub volume: String,
    #[serde(rename = "q")]
    pub quote_volume: String,
    #[serde(rename = "x")]
    pub is_closed: bool,
}

impl WsKlineData {
    pub fn to_candle(&self) -> Candle {
        Candle {
            open_time: self.open_time,
            open: self.open.parse().unwrap_or(0.0),
            high: self.high.parse().unwrap_or(0.0),
            low: self.low.parse().unwrap_or(0.0),
            close: self.close.parse().unwrap_or(0.0),
            volume: self.volume.parse().unwrap_or(0.0),
            quote_volume: self.quote_volume.parse().unwrap_or(0.0),
            is_closed: self.is_closed,
        }
    }
}

/// Force Order (Liquidation) WebSocket event (`!forceOrder@arr`)
#[derive(Debug, Clone, Deserialize)]
pub struct WsForceOrderEvent {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "o")]
    pub order: ForceOrderDetail,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ForceOrderDetail {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "S")]
    pub side: String, // "SELL" means Long liquidated, "BUY" means Short liquidated
    #[serde(rename = "q")]
    pub orig_qty: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "ap")]
    pub avg_price: String,
    #[serde(rename = "T")]
    pub trade_time: i64,
}

impl ForceOrderDetail {
    pub fn price_f64(&self) -> f64 {
        let ap = self.avg_price.parse::<f64>().unwrap_or(0.0);
        if ap > 0.0 {
            ap
        } else {
            self.price.parse::<f64>().unwrap_or(0.0)
        }
    }

    pub fn qty_f64(&self) -> f64 {
        self.orig_qty.parse::<f64>().unwrap_or(0.0)
    }

    pub fn usd_value(&self) -> f64 {
        self.price_f64() * self.qty_f64()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Candle {
    pub open_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub is_closed: bool,
}

impl Candle {
    pub fn change_pct(&self) -> f64 {
        if self.open <= 0.0 {
            0.0
        } else {
            ((self.close - self.open) / self.open) * 100.0
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SignalDirection {
    Bullish,
    Bearish,
    Neutral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SignalCategory {
    PricePump,
    PriceDump,
    VolumeSpike,
    RsiOverbought,
    RsiOversold,
    EmaBullishCross,
    EmaBearishCross,
    BollingerUpperBreakout,
    BollingerLowerBreakout,
    FundingRateExtreme,
    OpenInterestSpike,
    LargeLiquidation,
}

impl SignalCategory {
    pub fn as_key(&self) -> &'static str {
        match self {
            Self::PricePump => "PRICE_PUMP",
            Self::PriceDump => "PRICE_DUMP",
            Self::VolumeSpike => "VOLUME_SPIKE",
            Self::RsiOverbought => "RSI_OVERBOUGHT",
            Self::RsiOversold => "RSI_OVERSOLD",
            Self::EmaBullishCross => "EMA_BULL_CROSS",
            Self::EmaBearishCross => "EMA_BEAR_CROSS",
            Self::BollingerUpperBreakout => "BB_UPPER_BREAK",
            Self::BollingerLowerBreakout => "BB_LOWER_BREAK",
            Self::FundingRateExtreme => "FUNDING_EXTREME",
            Self::OpenInterestSpike => "OI_SPIKE",
            Self::LargeLiquidation => "LIQUIDATION",
        }
    }

    pub fn title_th(&self) -> &'static str {
        match self {
            Self::PricePump => "🚀 ราคาพุ่งแรง (Price Pump)",
            Self::PriceDump => "🩸 ราคาทุบแรง (Price Dump)",
            Self::VolumeSpike => "📊 วอลุ่มพุ่งผิดปกติ (Volume Spike)",
            Self::RsiOverbought => "🔥 RSI เข้าเขตซื้อมากเกินไป (Overbought)",
            Self::RsiOversold => "🧊 RSI เข้าเขตขายมากเกินไป (Oversold)",
            Self::EmaBullishCross => "📈 EMA Golden Cross (ตัดขึ้น)",
            Self::EmaBearishCross => "📉 EMA Death Cross (ตัดลง)",
            Self::BollingerUpperBreakout => "⚡ ทะลุกรอบบน Bollinger Band + Volume",
            Self::BollingerLowerBreakout => "⚠️ หลุดกรอบล่าง Bollinger Band + Volume",
            Self::FundingRateExtreme => "💰 Funding Rate สูง/ต่ำผิดปกติ",
            Self::OpenInterestSpike => "🧲 Open Interest (OI) เปลี่ยนแปลงฉับพลัน",
            Self::LargeLiquidation => "💥 ล้างพอร์ตรายใหญ่ (Large Liquidation)",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanSignal {
    pub symbol: String,
    pub category: SignalCategory,
    pub direction: SignalDirection,
    pub price: f64,
    pub change_24h_pct: Option<f64>,
    pub quote_volume_24h: Option<f64>,
    pub timeframe: String,
    pub metric_summary: String,
    pub details: Vec<(String, String)>,
    pub timestamp: DateTime<Utc>,
}

impl ScanSignal {
    pub fn binance_futures_url(&self) -> String {
        format!("https://www.binance.com/en/futures/{}", self.symbol)
    }

    pub fn discord_color(&self) -> u32 {
        match self.direction {
            SignalDirection::Bullish => 0x00E676, // Bright Green
            SignalDirection::Bearish => 0xFF5252, // Bright Red
            SignalDirection::Neutral => 0xFFB300, // Amber
        }
    }
}
