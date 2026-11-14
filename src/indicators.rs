use crate::models::Candle;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CrossoverType {
    Bullish,
    Bearish,
}

#[derive(Debug, Clone, Copy)]
pub struct EmaCrossoverResult {
    pub crossover: Option<CrossoverType>,
    pub fast_now: f64,
    pub slow_now: f64,
    pub fast_prev: f64,
    pub slow_prev: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct BollingerBandsResult {
    pub upper: f64,
    pub middle: f64,
    pub lower: f64,
    pub bandwidth_pct: f64,
    pub percent_b: f64,
}

/// Calculates Wilder's RSI for the given closing prices and period.
pub fn calculate_rsi(closes: &[f64], period: usize) -> Option<f64> {
    if period == 0 || closes.len() <= period {
        return None;
    }

    let mut gains = 0.0;
    let mut losses = 0.0;

    for i in 1..=period {
        let diff = closes[i] - closes[i - 1];
        if diff > 0.0 {
            gains += diff;
        } else {
            losses -= diff;
        }
    }

    let period_f = period as f64;
    let mut avg_gain = gains / period_f;
    let mut avg_loss = losses / period_f;

    for i in (period + 1)..closes.len() {
        let diff = closes[i] - closes[i - 1];
        let (gain, loss) = if diff > 0.0 {
            (diff, 0.0)
        } else {
            (0.0, -diff)
        };
        avg_gain = ((avg_gain * (period_f - 1.0)) + gain) / period_f;
        avg_loss = ((avg_loss * (period_f - 1.0)) + loss) / period_f;
    }

    if avg_loss == 0.0 {
        return Some(100.0);
    }

    let rs = avg_gain / avg_loss;
    Some(100.0 - (100.0 / (1.0 + rs)))
}

/// Calculates Exponential Moving Average (EMA) series.
pub fn calculate_ema_series(closes: &[f64], period: usize) -> Vec<f64> {
    if period == 0 || closes.len() < period {
        return Vec::new();
    }

    let mut ema_values = Vec::with_capacity(closes.len());
    let sma: f64 = closes[..period].iter().sum::<f64>() / (period as f64);

    // Fill initial values with SMA up to `period - 1` so indices align with `closes`
    for _ in 0..period {
        ema_values.push(sma);
    }

    let k = 2.0 / ((period as f64) + 1.0);
    let mut prev_ema = sma;

    for &price in &closes[period..] {
        let ema = (price - prev_ema) * k + prev_ema;
        ema_values.push(ema);
        prev_ema = ema;
    }

    ema_values
}

/// Detects EMA crossover on the latest candle compared to the previous candle.
pub fn detect_ema_crossover(
    closes: &[f64],
    fast_period: usize,
    slow_period: usize,
) -> Option<EmaCrossoverResult> {
    if fast_period == 0 || slow_period == 0 || fast_period >= slow_period {
        return None;
    }
    if closes.len() < slow_period + 2 {
        return None;
    }

    let fast_series = calculate_ema_series(closes, fast_period);
    let slow_series = calculate_ema_series(closes, slow_period);

    let len = closes.len();
    let fast_prev = fast_series[len - 2];
    let slow_prev = slow_series[len - 2];
    let fast_now = fast_series[len - 1];
    let slow_now = slow_series[len - 1];

    let crossover = if fast_prev <= slow_prev && fast_now > slow_now {
        Some(CrossoverType::Bullish)
    } else if fast_prev >= slow_prev && fast_now < slow_now {
        Some(CrossoverType::Bearish)
    } else {
        None
    };

    Some(EmaCrossoverResult {
        crossover,
        fast_now,
        slow_now,
        fast_prev,
        slow_prev,
    })
}

/// Calculates Bollinger Bands for the latest data point.
pub fn calculate_bollinger_bands(
    closes: &[f64],
    period: usize,
    std_dev_mult: f64,
) -> Option<BollingerBandsResult> {
    if period == 0 || closes.len() < period {
        return None;
    }

    let window = &closes[closes.len() - period..];
    let mean = window.iter().sum::<f64>() / (period as f64);

    let variance = window
        .iter()
        .map(|&price| {
            let diff = price - mean;
            diff * diff
        })
        .sum::<f64>()
        / (period as f64);

    let std_dev = variance.sqrt();
    let upper = mean + std_dev_mult * std_dev;
    let lower = mean - std_dev_mult * std_dev;

    let last_close = *closes.last()?;
    let band_width = upper - lower;
    let bandwidth_pct = if mean > 0.0 {
        (band_width / mean) * 100.0
    } else {
        0.0
    };
    let percent_b = if band_width > 0.0 {
        (last_close - lower) / band_width
    } else {
        0.5
    };

    Some(BollingerBandsResult {
        upper,
        middle: mean,
        lower,
        bandwidth_pct,
        percent_b,
    })
}

/// Calculates the ratio of the latest candle's quote volume against the average of the preceding `lookback` candles.
pub fn calculate_volume_ratio(candles: &[Candle], lookback: usize) -> Option<(f64, f64)> {
    if lookback == 0 || candles.len() < lookback + 1 {
        return None;
    }

    let latest = candles.last()?;
    let history = &candles[candles.len() - 1 - lookback..candles.len() - 1];
    let avg_quote_vol =
        history.iter().map(|c| c.quote_volume).sum::<f64>() / (lookback as f64);

    if avg_quote_vol <= 0.0 {
        return None;
    }

    let ratio = latest.quote_volume / avg_quote_vol;
    Some((ratio, avg_quote_vol))
}

/// Calculates Average True Range (ATR) over `period` candles.
pub fn calculate_atr(candles: &[Candle], period: usize) -> Option<f64> {
    if period == 0 || candles.len() < 2 {
        return None;
    }

    let mut trs = Vec::with_capacity(candles.len() - 1);
    for i in 1..candles.len() {
        let h = candles[i].high;
        let l = candles[i].low;
        let prev_c = candles[i - 1].close;
        let tr = (h - l).max((h - prev_c).abs()).max((l - prev_c).abs());
        trs.push(tr);
    }

    if trs.is_empty() {
        return None;
    }

    let window = if trs.len() >= period {
        &trs[trs.len() - period..]
    } else {
        &trs[..]
    };

    Some(window.iter().sum::<f64>() / (window.len() as f64))
}

/// Appends Entry Zone, TP1 (1.5R), TP2 (2.5R), and Stop Loss (SL) details based on ATR and Swing High/Low.
pub fn append_trade_plan(
    details: &mut Vec<(String, String)>,
    direction: crate::models::SignalDirection,
    price: f64,
    candles: Option<&[Candle]>,
) {
    use crate::models::SignalDirection;
    use crate::notifier::format_price;

    if price <= 0.0 || direction == SignalDirection::Neutral {
        return;
    }

    let atr = candles
        .and_then(|c| calculate_atr(c, 14))
        .filter(|&v| v > 0.0)
        .unwrap_or(price * 0.012);

    let mut raw_risk = atr * 1.5;

    if let Some(c_slice) = candles {
        if c_slice.len() >= 6 {
            let start = c_slice.len().saturating_sub(10);
            let recent = &c_slice[start..];
            match direction {
                SignalDirection::Bullish => {
                    if let Some(swing_low) = recent
                        .iter()
                        .map(|c| c.low)
                        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    {
                        let swing_dist = (price - swing_low) + 0.25 * atr;
                        if swing_dist > 0.0 {
                            raw_risk = raw_risk.max(swing_dist);
                        }
                    }
                }
                SignalDirection::Bearish => {
                    if let Some(swing_high) = recent
                        .iter()
                        .map(|c| c.high)
                        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    {
                        let swing_dist = (swing_high - price) + 0.25 * atr;
                        if swing_dist > 0.0 {
                            raw_risk = raw_risk.max(swing_dist);
                        }
                    }
                }
                SignalDirection::Neutral => {}
            }
        }
    }

    let min_risk = price * 0.006; // 0.6%
    let max_risk = price * 0.045; // 4.5%
    let risk = raw_risk.clamp(min_risk, max_risk);
    let risk_pct = (risk / price) * 100.0;

    match direction {
        SignalDirection::Bullish => {
            let entry_low = price - 0.25 * atr;
            let sl = price - risk;
            let tp1 = price + risk * 1.5;
            let tp2 = price + risk * 2.5;
            let tp1_pct = ((tp1 - price) / price) * 100.0;
            let tp2_pct = ((tp2 - price) / price) * 100.0;

            details.push((
                "🧭 แผนเทรด".to_string(),
                "🟢 เปิด LONG (เก็งกำไรขาขึ้น)".to_string(),
            ));
            details.push((
                "🎯 จุดเข้า (Entry)".to_string(),
                format!("${} - ${}", format_price(entry_low), format_price(price)),
            ));
            details.push((
                "💰 เป้าทำกำไร TP1".to_string(),
                format!("${} (+{:.2}%)", format_price(tp1), tp1_pct),
            ));
            details.push((
                "💰 เป้าทำกำไร TP2".to_string(),
                format!("${} (+{:.2}%)", format_price(tp2), tp2_pct),
            ));
            details.push((
                "🛡️ ตัดขาดทุน (SL)".to_string(),
                format!("${} (-{:.2}%)", format_price(sl), risk_pct),
            ));
        }
        SignalDirection::Bearish => {
            let entry_high = price + 0.25 * atr;
            let sl = price + risk;
            let tp1 = price - risk * 1.5;
            let tp2 = price - risk * 2.5;
            let tp1_pct = ((price - tp1) / price) * 100.0;
            let tp2_pct = ((price - tp2) / price) * 100.0;

            details.push((
                "🧭 แผนเทรด".to_string(),
                "🔴 เปิด SHORT (เก็งกำไรขาลง)".to_string(),
            ));
            details.push((
                "🎯 จุดเข้า (Entry)".to_string(),
                format!("${} - ${}", format_price(price), format_price(entry_high)),
            ));
            details.push((
                "💰 เป้าทำกำไร TP1".to_string(),
                format!("${} (+{:.2}%)", format_price(tp1), tp1_pct),
            ));
            details.push((
                "💰 เป้าทำกำไร TP2".to_string(),
                format!("${} (+{:.2}%)", format_price(tp2), tp2_pct),
            ));
            details.push((
                "🛡️ ตัดขาดทุน (SL)".to_string(),
                format!("${} (-{:.2}%)", format_price(sl), risk_pct),
            ));
        }
        SignalDirection::Neutral => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rsi_calculation() {
        let mut up_prices = Vec::new();
        for i in 1..=20 {
            up_prices.push(100.0 + (i as f64) * 2.0);
        }
        let rsi = calculate_rsi(&up_prices, 14).unwrap();
        assert!((rsi - 100.0).abs() < 1e-6);

        let mut down_prices = Vec::new();
        for i in 1..=20 {
            down_prices.push(200.0 - (i as f64) * 2.0);
        }
        let rsi_down = calculate_rsi(&down_prices, 14).unwrap();
        assert!(rsi_down < 1.0);
    }

    #[test]
    fn test_ema_crossover_detection() {
        let mut closes = vec![100.0; 23];
        // At index 20, both EMA(9) and EMA(21) equal 100.0.
        // Dip at index 21 makes fast EMA < slow EMA
        closes[21] = 95.0;
        // Surge at index 22 makes fast EMA > slow EMA (Bullish crossover)
        closes[22] = 110.0;

        let res = detect_ema_crossover(&closes, 9, 21).unwrap();
        assert_eq!(res.crossover, Some(CrossoverType::Bullish));

        // Now test Bearish crossover
        let mut closes_bear = vec![100.0; 23];
        closes_bear[21] = 105.0;
        closes_bear[22] = 90.0;
        let res_bear = detect_ema_crossover(&closes_bear, 9, 21).unwrap();
        assert_eq!(res_bear.crossover, Some(CrossoverType::Bearish));
    }

    #[test]
    fn test_bollinger_bands() {
        let closes: Vec<f64> = (1..=20).map(|x| 100.0 + (x as f64)).collect();
        let bb = calculate_bollinger_bands(&closes, 20, 2.0).unwrap();
        assert!(bb.upper > bb.middle);
        assert!(bb.middle > bb.lower);
        assert!(bb.bandwidth_pct > 0.0);
    }
}
