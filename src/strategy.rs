use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::models::SignalCategory;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum TradingStrategy {
    #[default]
    Balanced,
    TrendFollow,
    Breakout,
    ReversalSniper,
    ScalpQuick,
}

#[derive(Debug, Clone, Copy)]
pub struct StrategyTradeProfile {
    pub min_risk_pct: f64,
    pub max_risk_pct: f64,
    pub atr15_mult: f64,
    pub tp1_r: f64,
    pub tp2_r: f64,
    pub tp3_r: f64,
    pub tp1_atr5_mult: f64,
    pub tp2_atr15_mult: f64,
    pub tp3_atr1h_mult: f64,
    pub tp1_close_pct: f64,
    pub tp2_close_pct: f64,
    pub tp3_close_pct: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct StrategyPerfStats {
    #[serde(default)]
    pub trades: u64,
    #[serde(default)]
    pub wins: u64,
    #[serde(default)]
    pub losses: u64,
    #[serde(default)]
    pub realized_pnl: f64,
}

impl StrategyPerfStats {
    pub fn win_rate_pct(&self) -> f64 {
        if self.trades == 0 {
            0.0
        } else {
            (self.wins as f64 / self.trades as f64) * 100.0
        }
    }
}

pub fn record_strategy_closed_trade(
    stats: &mut HashMap<String, StrategyPerfStats>,
    strategy_key: &str,
    net_pnl: f64,
) {
    let canonical = TradingStrategy::from_str_lossy(strategy_key)
        .unwrap_or(TradingStrategy::Balanced)
        .key()
        .to_string();
    let entry = stats.entry(canonical).or_default();
    entry.trades += 1;
    if net_pnl >= 0.0 {
        entry.wins += 1;
    } else {
        entry.losses += 1;
    }
    entry.realized_pnl += net_pnl;
}

impl TradingStrategy {
    pub fn all() -> &'static [TradingStrategy] {
        &[
            Self::Balanced,
            Self::TrendFollow,
            Self::Breakout,
            Self::ReversalSniper,
            Self::ScalpQuick,
        ]
    }

    pub fn key(&self) -> &'static str {
        match self {
            Self::Balanced => "BALANCED",
            Self::TrendFollow => "TREND_FOLLOW",
            Self::Breakout => "BREAKOUT",
            Self::ReversalSniper => "REVERSAL_SNIPER",
            Self::ScalpQuick => "SCALP_QUICK",
        }
    }

    pub fn short_tag(&self) -> &'static str {
        match self {
            Self::Balanced => "Balanced",
            Self::TrendFollow => "Trend",
            Self::Breakout => "Breakout",
            Self::ReversalSniper => "Sniper",
            Self::ScalpQuick => "Scalp",
        }
    }

    pub fn title_th(&self) -> &'static str {
        match self {
            Self::Balanced => "⚖️ Balanced (สมดุล)",
            Self::TrendFollow => "📈 Trend Follow",
            Self::Breakout => "💥 Breakout",
            Self::ReversalSniper => "🎯 Reversal Sniper",
            Self::ScalpQuick => "⚡ Scalp Quick",
        }
    }

    pub fn description_th(&self) -> &'static str {
        match self {
            Self::Balanced => "ครบทุกสัญญาณ สมดุลทั้งตามเทรนด์และรอบสั้น (ค่ามาตรฐานระบบ)",
            Self::TrendFollow => "ตามเทรนด์ใหญ่ 1h เท่านั้น (EMA/BB/Vol) รันเป้าไกล ปิดหนัก TP2/TP3",
            Self::Breakout => "ดักทะลุกรอบ BB + แท่งกระชาก + วอลุ่ม/OI พุ่ง เก็บโมเมนตัมแรง",
            Self::ReversalSniper => "ดักจุดกลับตัวสุดขั้ว (RSI OB/OS + Funding + Liq) SL กระชับ เก็บแม่น",
            Self::ScalpQuick => "เล่นรอบสั้นไว 5m/15m จำกัด SL แคบ เน้นเก็บกำไรไวที่ TP1/TP2",
        }
    }

    pub fn signals_summary_th(&self) -> &'static str {
        match self {
            Self::Balanced => "ทุกสัญญาณ (Pump/Dump, Vol, RSI, EMA, BB, Funding, OI, Liq)",
            Self::TrendFollow => "EMA Cross, BB Breakout, Volume Spike (บังคับตามเทรนด์ 1h)",
            Self::Breakout => "BB Breakout, Pump/Dump, Volume Spike (+ OI Spike)",
            Self::ReversalSniper => "RSI Overbought/Oversold, Funding Extreme (+ Liquidation)",
            Self::ScalpQuick => "EMA Cross, RSI OB/OS, BB Breakout, Volume Spike, Pump/Dump",
        }
    }

    pub fn default_min_stars(&self, env_min_stars: u8) -> u8 {
        match self {
            Self::Balanced => env_min_stars.clamp(1, 3),
            Self::TrendFollow => 3,
            Self::Breakout => 2,
            Self::ReversalSniper => 3,
            Self::ScalpQuick => 2,
        }
    }

    pub fn requires_1h_alignment(&self) -> bool {
        matches!(self, Self::TrendFollow)
    }

    pub fn allows_category(&self, cat: SignalCategory) -> bool {
        match self {
            Self::Balanced => true,
            Self::TrendFollow => matches!(
                cat,
                SignalCategory::EmaBullishCross
                    | SignalCategory::EmaBearishCross
                    | SignalCategory::BollingerUpperBreakout
                    | SignalCategory::BollingerLowerBreakout
                    | SignalCategory::VolumeSpike
            ),
            Self::Breakout => matches!(
                cat,
                SignalCategory::BollingerUpperBreakout
                    | SignalCategory::BollingerLowerBreakout
                    | SignalCategory::PricePump
                    | SignalCategory::PriceDump
                    | SignalCategory::VolumeSpike
                    | SignalCategory::OpenInterestSpike
            ),
            Self::ReversalSniper => matches!(
                cat,
                SignalCategory::RsiOverbought
                    | SignalCategory::RsiOversold
                    | SignalCategory::FundingRateExtreme
                    | SignalCategory::LargeLiquidation
            ),
            Self::ScalpQuick => matches!(
                cat,
                SignalCategory::EmaBullishCross
                    | SignalCategory::EmaBearishCross
                    | SignalCategory::RsiOverbought
                    | SignalCategory::RsiOversold
                    | SignalCategory::BollingerUpperBreakout
                    | SignalCategory::BollingerLowerBreakout
                    | SignalCategory::VolumeSpike
                    | SignalCategory::PricePump
                    | SignalCategory::PriceDump
            ),
        }
    }

    pub fn trade_profile(&self) -> StrategyTradeProfile {
        match self {
            Self::Balanced => StrategyTradeProfile {
                min_risk_pct: 0.70,
                max_risk_pct: 4.80,
                atr15_mult: 1.40,
                tp1_r: 1.3,
                tp2_r: 2.3,
                tp3_r: 3.6,
                tp1_atr5_mult: 2.2,
                tp2_atr15_mult: 2.2,
                tp3_atr1h_mult: 1.8,
                tp1_close_pct: 40.0,
                tp2_close_pct: 30.0,
                tp3_close_pct: 30.0,
            },
            Self::TrendFollow => StrategyTradeProfile {
                min_risk_pct: 0.80,
                max_risk_pct: 4.80,
                atr15_mult: 1.50,
                tp1_r: 1.5,
                tp2_r: 2.8,
                tp3_r: 4.5,
                tp1_atr5_mult: 2.4,
                tp2_atr15_mult: 2.6,
                tp3_atr1h_mult: 2.2,
                tp1_close_pct: 25.0,
                tp2_close_pct: 35.0,
                tp3_close_pct: 40.0,
            },
            Self::Breakout => StrategyTradeProfile {
                min_risk_pct: 0.70,
                max_risk_pct: 4.20,
                atr15_mult: 1.35,
                tp1_r: 1.3,
                tp2_r: 2.4,
                tp3_r: 3.8,
                tp1_atr5_mult: 2.2,
                tp2_atr15_mult: 2.3,
                tp3_atr1h_mult: 1.9,
                tp1_close_pct: 35.0,
                tp2_close_pct: 35.0,
                tp3_close_pct: 30.0,
            },
            Self::ReversalSniper => StrategyTradeProfile {
                min_risk_pct: 0.65,
                max_risk_pct: 3.80,
                atr15_mult: 1.25,
                tp1_r: 1.2,
                tp2_r: 2.0,
                tp3_r: 3.0,
                tp1_atr5_mult: 2.0,
                tp2_atr15_mult: 2.0,
                tp3_atr1h_mult: 1.5,
                tp1_close_pct: 50.0,
                tp2_close_pct: 35.0,
                tp3_close_pct: 15.0,
            },
            Self::ScalpQuick => StrategyTradeProfile {
                min_risk_pct: 0.55,
                max_risk_pct: 2.80,
                atr15_mult: 1.15,
                tp1_r: 1.0,
                tp2_r: 1.7,
                tp3_r: 2.5,
                tp1_atr5_mult: 1.7,
                tp2_atr15_mult: 1.8,
                tp3_atr1h_mult: 1.4,
                tp1_close_pct: 50.0,
                tp2_close_pct: 35.0,
                tp3_close_pct: 15.0,
            },
        }
    }

    pub fn from_str_lossy(raw: &str) -> Option<Self> {
        let norm = raw.trim().to_lowercase().replace(['-', ' '], "_");
        match norm.as_str() {
            "balanced" | "bal" | "all" | "default" | "standard" | "สมดุล" | "มาตรฐาน" | "รวม" => {
                Some(Self::Balanced)
            }
            "trend_follow" | "trendfollow" | "trend" | "tf" | "ตามเทรนด์" => {
                Some(Self::TrendFollow)
            }
            "breakout" | "break" | "bo" | "momentum" | "ทะลุกรอบ" => Some(Self::Breakout),
            "reversal_sniper" | "reversalsniper" | "reversal" | "sniper" | "rev" | "กลับตัว"
            | "สวน" => Some(Self::ReversalSniper),
            "scalp_quick" | "scalpquick" | "scalp" | "scalping" | "quick" | "สั้น" | "ไว" => {
                Some(Self::ScalpQuick)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_parsing_and_profiles() {
        assert_eq!(
            TradingStrategy::from_str_lossy("trend"),
            Some(TradingStrategy::TrendFollow)
        );
        assert_eq!(
            TradingStrategy::from_str_lossy("Reversal-Sniper"),
            Some(TradingStrategy::ReversalSniper)
        );
        assert_eq!(
            TradingStrategy::from_str_lossy("scalp"),
            Some(TradingStrategy::ScalpQuick)
        );
        assert_eq!(
            TradingStrategy::from_str_lossy("breakout"),
            Some(TradingStrategy::Breakout)
        );
        assert_eq!(
            TradingStrategy::from_str_lossy("balanced"),
            Some(TradingStrategy::Balanced)
        );
        assert_eq!(TradingStrategy::from_str_lossy("unknown_xyz"), None);

        for s in TradingStrategy::all() {
            let p = s.trade_profile();
            let sum_pct = p.tp1_close_pct + p.tp2_close_pct + p.tp3_close_pct;
            assert!((sum_pct - 100.0).abs() < 1e-6);
            assert!(p.min_risk_pct < p.max_risk_pct);
            assert!(p.tp1_r < p.tp2_r && p.tp2_r < p.tp3_r);
        }

        assert!(TradingStrategy::TrendFollow.requires_1h_alignment());
        assert_eq!(TradingStrategy::TrendFollow.default_min_stars(2), 3);
        assert_eq!(TradingStrategy::ReversalSniper.default_min_stars(2), 3);
        assert_eq!(TradingStrategy::ScalpQuick.default_min_stars(3), 2);
        assert_eq!(TradingStrategy::Balanced.default_min_stars(2), 2);
    }

    #[test]
    fn test_strategy_signal_filters() {
        assert!(TradingStrategy::TrendFollow.allows_category(SignalCategory::EmaBullishCross));
        assert!(!TradingStrategy::TrendFollow.allows_category(SignalCategory::RsiOversold));
        assert!(!TradingStrategy::TrendFollow.allows_category(SignalCategory::FundingRateExtreme));

        assert!(TradingStrategy::Breakout.allows_category(SignalCategory::BollingerUpperBreakout));
        assert!(TradingStrategy::Breakout.allows_category(SignalCategory::PricePump));
        assert!(!TradingStrategy::Breakout.allows_category(SignalCategory::EmaBullishCross));

        assert!(TradingStrategy::ReversalSniper.allows_category(SignalCategory::RsiOversold));
        assert!(TradingStrategy::ReversalSniper.allows_category(SignalCategory::FundingRateExtreme));
        assert!(!TradingStrategy::ReversalSniper.allows_category(SignalCategory::VolumeSpike));

        assert!(TradingStrategy::ScalpQuick.allows_category(SignalCategory::EmaBullishCross));
        assert!(TradingStrategy::ScalpQuick.allows_category(SignalCategory::RsiOversold));
        assert!(!TradingStrategy::ScalpQuick.allows_category(SignalCategory::FundingRateExtreme));
    }
}
