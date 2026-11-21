use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

use crate::config::ScannerConfig;
use crate::models::{
    Candle, CombinedStreamWrapper, ExchangeInfoResponse, OpenInterestResponse, PremiumIndex,
    Ticker24hr, WsForceOrderEvent, WsKlineEvent,
};

pub struct MarketSnapshot {
    pub symbols: Vec<String>,
    pub qualified_tickers: HashMap<String, Ticker24hr>,
    pub all_prices: HashMap<String, f64>,
}

#[derive(Clone)]
pub struct BinanceFuturesClient {
    config: ScannerConfig,
    http: Client,
}

impl BinanceFuturesClient {
    pub fn new(config: ScannerConfig) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent("scanner-f-rust/0.1.0")
            .build()
            .expect("Failed to initialize reqwest client");

        Self { config, http }
    }

    /// Returns active USDT-M Perpetual symbols filtered by minimum 24h quote volume,
    /// sorted by descending 24h quote volume, plus prices for existing paper positions.
    pub async fn fetch_market_snapshot(&self) -> Result<MarketSnapshot> {
        let exchange_info_url = format!("{}/fapi/v1/exchangeInfo", self.config.rest_base_url);
        let resp = self
            .http
            .get(&exchange_info_url)
            .send()
            .await
            .context("Failed to request /fapi/v1/exchangeInfo")?;

        if resp.status().as_u16() == 451 {
            anyhow::bail!(
                "Binance Futures ปฏิเสธการเชื่อมต่อ (HTTP 451 Restricted Location) เนื่องจาก IP ของเซิร์ฟเวอร์ (เช่น GitHub Actions US Runner) อยู่ในภูมิภาคที่ Binance บล็อก — แนะนำให้ตั้งค่า Secret `HTTPS_PROXY` หรือ `BINANCE_FAPI_URL` (Cloudflare Worker / Reverse Proxy นอกสหรัฐฯ) หรือใช้ Self-hosted Runner"
            );
        }

        let exchange_info: ExchangeInfoResponse = resp
            .error_for_status()
            .context("Binance /fapi/v1/exchangeInfo returned error status")?
            .json()
            .await
            .context("Failed to parse /fapi/v1/exchangeInfo JSON")?;

        let valid_perps: HashSet<String> = exchange_info
            .symbols
            .into_iter()
            .filter(|s| {
                s.contract_type == "PERPETUAL"
                    && s.status == "TRADING"
                    && s.quote_asset == "USDT"
            })
            .map(|s| s.symbol)
            .collect();

        let ticker_url = format!("{}/fapi/v1/ticker/24hr", self.config.rest_base_url);
        let tickers: Vec<Ticker24hr> = self
            .http
            .get(&ticker_url)
            .send()
            .await
            .context("Failed to request /fapi/v1/ticker/24hr")?
            .error_for_status()
            .context("Binance /fapi/v1/ticker/24hr returned error status")?
            .json()
            .await
            .context("Failed to parse /fapi/v1/ticker/24hr JSON")?;

        Ok(build_market_snapshot(
            tickers,
            &valid_perps,
            self.config.min_quote_volume_24h,
            self.config.max_symbols,
        ))
    }

    /// Fetches historical klines (`/fapi/v1/klines`) for a symbol.
    pub async fn fetch_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: usize,
    ) -> Result<Vec<Candle>> {
        let url = format!("{}/fapi/v1/klines", self.config.rest_base_url);
        let rows: Vec<Vec<Value>> = self
            .http
            .get(&url)
            .query(&[
                ("symbol", symbol),
                ("interval", interval),
                ("limit", &limit.to_string()),
            ])
            .send()
            .await
            .with_context(|| format!("Failed to fetch klines for {}", symbol))?
            .error_for_status()
            .with_context(|| format!("Error status fetching klines for {}", symbol))?
            .json()
            .await
            .with_context(|| format!("Invalid kline JSON for {}", symbol))?;

        let mut candles = Vec::with_capacity(rows.len());
        let total = rows.len();

        for (idx, row) in rows.into_iter().enumerate() {
            if row.len() < 8 {
                continue;
            }
            let open_time = row[0].as_i64().unwrap_or(0);
            let open = value_to_f64(&row[1]);
            let high = value_to_f64(&row[2]);
            let low = value_to_f64(&row[3]);
            let close = value_to_f64(&row[4]);
            let volume = value_to_f64(&row[5]);
            let quote_volume = value_to_f64(&row[7]);
            // All historical klines except the last currently forming one are closed
            let is_closed = idx + 1 < total;

            candles.push(Candle {
                open_time,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                is_closed,
            });
        }

        Ok(candles)
    }

    /// Fetches Premium Index & Funding Rate for all symbols (`/fapi/v1/premiumIndex`).
    pub async fn fetch_all_premium_index(&self) -> Result<Vec<PremiumIndex>> {
        let url = format!("{}/fapi/v1/premiumIndex", self.config.rest_base_url);
        let list: Vec<PremiumIndex> = self
            .http
            .get(&url)
            .send()
            .await
            .context("Failed to fetch /fapi/v1/premiumIndex")?
            .error_for_status()
            .context("Error status from /fapi/v1/premiumIndex")?
            .json()
            .await
            .context("Failed to parse /fapi/v1/premiumIndex JSON")?;
        Ok(list)
    }

    /// Fetches current Open Interest for a specific symbol (`/fapi/v1/openInterest`).
    pub async fn fetch_open_interest(&self, symbol: &str) -> Result<OpenInterestResponse> {
        let url = format!("{}/fapi/v1/openInterest", self.config.rest_base_url);
        let resp: OpenInterestResponse = self
            .http
            .get(&url)
            .query(&[("symbol", symbol)])
            .send()
            .await
            .with_context(|| format!("Failed to fetch openInterest for {}", symbol))?
            .error_for_status()
            .with_context(|| format!("Error status on openInterest for {}", symbol))?
            .json()
            .await
            .with_context(|| format!("Failed to parse openInterest JSON for {}", symbol))?;
        Ok(resp)
    }

    /// Fetches lightweight latest prices for all symbols (`/fapi/v1/ticker/price`).
    pub async fn fetch_all_prices(&self) -> Result<HashMap<String, f64>> {
        let url = format!("{}/fapi/v1/ticker/price", self.config.rest_base_url);
        let rows: Vec<Value> = self
            .http
            .get(&url)
            .send()
            .await
            .context("Failed to fetch /fapi/v1/ticker/price")?
            .error_for_status()
            .context("Error status from /fapi/v1/ticker/price")?
            .json()
            .await
            .context("Failed to parse /fapi/v1/ticker/price JSON")?;

        let mut map = HashMap::with_capacity(rows.len());
        for row in rows {
            if let Some(sym) = row.get("symbol").and_then(|v| v.as_str()) {
                let price = value_to_f64(&row["price"]);
                if price > 0.0 {
                    map.insert(sym.to_string(), price);
                }
            }
        }
        Ok(map)
    }
}

/// Spawns a persistent, auto-reconnecting WebSocket worker for a batch of kline streams.
pub fn spawn_kline_ws_worker(
    ws_base_url: String,
    symbols: Vec<String>,
    interval: String,
    tx: mpsc::Sender<WsKlineEvent>,
) {
    tokio::spawn(async move {
        if symbols.is_empty() {
            return;
        }

        let stream_names: Vec<String> = symbols
            .iter()
            .map(|s| format!("{}@kline_{}", s.to_lowercase(), interval))
            .collect();
        let url = format!(
            "{}/stream?streams={}",
            ws_base_url.trim_end_matches('/'),
            stream_names.join("/")
        );

        loop {
            info!(
                "กำลังเชื่อมต่อ WebSocket Kline Stream ({} เหรียญ, TF {})...",
                symbols.len(),
                interval
            );

            match connect_async(&url).await {
                Ok((ws_stream, _)) => {
                    info!(
                        "เชื่อมต่อ WebSocket Kline Stream สำเร็จ ({} เหรียญ)",
                        symbols.len()
                    );
                    let (mut write, mut read) = ws_stream.split();

                    while let Some(msg_res) = read.next().await {
                        match msg_res {
                            Ok(Message::Text(text)) => {
                                if let Ok(wrapper) =
                                    serde_json::from_str::<CombinedStreamWrapper<WsKlineEvent>>(
                                        &text,
                                    )
                                {
                                    if tx.send(wrapper.data).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            Ok(Message::Ping(payload)) => {
                                let _ = write.send(Message::Pong(payload)).await;
                            }
                            Ok(Message::Close(frame)) => {
                                warn!("Kline WebSocket closed by server: {:?}", frame);
                                break;
                            }
                            Err(err) => {
                                error!("Kline WebSocket error: {:#}", err);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Err(err) => {
                    error!("ไม่สามารถเชื่อมต่อ Kline WebSocket ได้: {:#}", err);
                }
            }

            warn!("กำลังลองเชื่อมต่อ Kline WebSocket ใหม่ในอีก 5 วินาที...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

/// Spawns a persistent, auto-reconnecting WebSocket worker for All Market Liquidations (`!forceOrder@arr`).
pub fn spawn_liquidation_ws_worker(
    ws_base_url: String,
    tx: mpsc::Sender<WsForceOrderEvent>,
) {
    tokio::spawn(async move {
        let url = format!(
            "{}/ws/!forceOrder@arr",
            ws_base_url.trim_end_matches('/')
        );

        loop {
            info!("กำลังเชื่อมต่อ WebSocket Liquidation Stream (!forceOrder@arr)...");

            match connect_async(&url).await {
                Ok((ws_stream, _)) => {
                    info!("เชื่อมต่อ WebSocket Liquidation Stream สำเร็จ!");
                    let (mut write, mut read) = ws_stream.split();

                    while let Some(msg_res) = read.next().await {
                        match msg_res {
                            Ok(Message::Text(text)) => {
                                // !forceOrder@arr can send either a single object or an array of objects
                                if let Ok(single) =
                                    serde_json::from_str::<WsForceOrderEvent>(&text)
                                {
                                    if tx.send(single).await.is_err() {
                                        return;
                                    }
                                } else if let Ok(batch) =
                                    serde_json::from_str::<Vec<WsForceOrderEvent>>(&text)
                                {
                                    for ev in batch {
                                        if tx.send(ev).await.is_err() {
                                            return;
                                        }
                                    }
                                } else {
                                    debug!("Unrecognized forceOrder payload: {}", text);
                                }
                            }
                            Ok(Message::Ping(payload)) => {
                                let _ = write.send(Message::Pong(payload)).await;
                            }
                            Ok(Message::Close(frame)) => {
                                warn!("Liquidation WebSocket closed: {:?}", frame);
                                break;
                            }
                            Err(err) => {
                                error!("Liquidation WebSocket error: {:#}", err);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Err(err) => {
                    error!("ไม่สามารถเชื่อมต่อ Liquidation WebSocket ได้: {:#}", err);
                }
            }

            warn!("กำลังลองเชื่อมต่อ Liquidation WebSocket ใหม่ในอีก 5 วินาที...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

fn build_market_snapshot(
    tickers: Vec<Ticker24hr>,
    valid_perps: &HashSet<String>,
    min_quote_volume: f64,
    max_symbols: usize,
) -> MarketSnapshot {
    let mut all_prices = HashMap::with_capacity(tickers.len());
    let mut qualified = Vec::new();
    for ticker in tickers {
        let price = ticker.last_price_f64();
        if price.is_finite() && price > 0.0 {
            all_prices.insert(ticker.symbol.clone(), price);
        }
        if valid_perps.contains(&ticker.symbol) && ticker.quote_volume_f64() >= min_quote_volume {
            qualified.push(ticker);
        }
    }

    qualified.sort_by(|a, b| {
        b.quote_volume_f64()
            .partial_cmp(&a.quote_volume_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    qualified.truncate(max_symbols);

    MarketSnapshot {
        symbols: qualified.iter().map(|t| t.symbol.clone()).collect(),
        qualified_tickers: qualified
            .into_iter()
            .map(|t| (t.symbol.clone(), t))
            .collect(),
        all_prices,
    }
}

fn value_to_f64(val: &Value) -> f64 {
    if let Some(s) = val.as_str() {
        s.parse::<f64>().unwrap_or(0.0)
    } else if let Some(n) = val.as_f64() {
        n
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prices_include_symbols_excluded_from_the_scan() {
        let tickers = [
            ("TOPUSDT", "101", "300"),
            ("OUTRANKEDUSDT", "95", "200"),
            ("LOWVOLUSDT", "98", "50"),
            ("INVALIDUSDT", "NaN", "10"),
        ]
        .into_iter()
        .map(|(symbol, last_price, quote_volume)| Ticker24hr {
            symbol: symbol.to_string(),
            last_price: last_price.to_string(),
            price_change_percent: "0".to_string(),
            volume: "1".to_string(),
            quote_volume: quote_volume.to_string(),
        })
        .collect();
        let eligible = ["TOPUSDT", "OUTRANKEDUSDT", "LOWVOLUSDT"]
            .into_iter()
            .map(str::to_string)
            .collect();
        let snapshot = build_market_snapshot(tickers, &eligible, 100.0, 1);

        assert_eq!(snapshot.symbols, vec!["TOPUSDT".to_string()]);
        assert_eq!(snapshot.qualified_tickers.len(), 1);
        assert_eq!(snapshot.all_prices.get("OUTRANKEDUSDT"), Some(&95.0));
        assert_eq!(snapshot.all_prices.get("LOWVOLUSDT"), Some(&98.0));
        assert!(!snapshot.all_prices.contains_key("INVALIDUSDT"));
    }
}
