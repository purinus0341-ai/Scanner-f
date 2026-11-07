use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::config::ScannerConfig;
use crate::models::ScanSignal;

#[derive(Clone)]
pub struct AlertNotifier {
    config: ScannerConfig,
    http_client: Client,
    cooldowns: Arc<Mutex<HashMap<String, i64>>>,
}

impl AlertNotifier {
    pub fn new(config: ScannerConfig) -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to create HTTP client for AlertNotifier");

        Self {
            config,
            http_client,
            cooldowns: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn load_cooldowns(&self, saved: HashMap<String, i64>) {
        let mut map = self.cooldowns.lock().await;
        *map = saved;
    }

    pub async fn export_cooldowns(&self) -> HashMap<String, i64> {
        let now = Utc::now().timestamp();
        let cooldown_secs = self.config.alert_cooldown_secs as i64;
        let map = self.cooldowns.lock().await;
        map.iter()
            .filter(|(_, &ts)| now - ts < cooldown_secs)
            .map(|(k, &v)| (k.clone(), v))
            .collect()
    }

    /// Checks if the `(symbol, category)` pair is outside the cooldown window and sends the alert.
    pub async fn dispatch(&self, signal: ScanSignal) {
        let cooldown_secs = self.config.alert_cooldown_secs as i64;
        let key = format!("{}:{}", signal.symbol, signal.category.as_key());
        let now = Utc::now().timestamp();

        {
            let mut map = self.cooldowns.lock().await;
            if let Some(&last_sent) = map.get(&key) {
                if now - last_sent < cooldown_secs {
                    return;
                }
            }
            map.insert(key, now);
        }

        self.log_to_console(&signal);

        if self.config.has_telegram() {
            if let Err(err) = self.send_telegram(&signal).await {
                error!("Failed to send Telegram alert for {}: {:#}", signal.symbol, err);
            }
        }

        if self.config.has_discord() {
            if let Err(err) = self.send_discord(&signal).await {
                error!("Failed to send Discord alert for {}: {:#}", signal.symbol, err);
            }
        }
    }

    /// Sends a test notification immediately (bypassing cooldown) to verify Telegram / Discord setup.
    pub async fn send_test_notification(&self, signal: &ScanSignal) -> Result<()> {
        self.log_to_console(signal);

        if !self.config.has_telegram() && !self.config.has_discord() {
            warn!(
                "ยังไม่ได้ตั้งค่า TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID หรือ DISCORD_WEBHOOK_URL ใน .env (แสดงผลเฉพาะทาง Console)"
            );
        }

        if self.config.has_telegram() {
            self.send_telegram(signal).await?;
            info!("ส่งข้อความทดสอบไปยัง Telegram สำเร็จ!");
        }

        if self.config.has_discord() {
            self.send_discord(signal).await?;
            info!("ส่งข้อความทดสอบไปยัง Discord Webhook สำเร็จ!");
        }

        Ok(())
    }

    fn log_to_console(&self, signal: &ScanSignal) {
        let details_str = signal
            .details
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(" | ");

        info!(
            target: "scanner_alert",
            "[{}] {} ({}) | Price: ${} | {} | {}",
            signal.category.as_key(),
            signal.symbol,
            signal.timeframe,
            format_price(signal.price),
            signal.metric_summary,
            details_str
        );
    }

    async fn send_telegram(&self, signal: &ScanSignal) -> Result<()> {
        let token = match &self.config.telegram_bot_token {
            Some(t) => t,
            None => return Ok(()),
        };
        let chat_id = match &self.config.telegram_chat_id {
            Some(c) => c,
            None => return Ok(()),
        };

        let mut lines = Vec::new();
        lines.push(format!("<b>{}</b>", html_escape(signal.category.title_th())));
        lines.push(format!(
            "🪙 เหรียญ: <b>#{}</b> (TF: <code>{}</code>)",
            html_escape(&signal.symbol),
            html_escape(&signal.timeframe)
        ));
        lines.push(format!("💵 ราคาปัจจุบัน: <code>${}</code>", format_price(signal.price)));

        if let Some(chg24) = signal.change_24h_pct {
            let sign = if chg24 >= 0.0 { "+" } else { "" };
            lines.push(format!("📊 เปลี่ยนแปลง 24h: <code>{}{:.2}%</code>", sign, chg24));
        }
        if let Some(vol24) = signal.quote_volume_24h {
            lines.push(format!("💧 วอลุ่ม 24h: <code>${}</code>", format_compact_usd(vol24)));
        }

        lines.push(format!("📌 สัญญาณ: <b>{}</b>", html_escape(&signal.metric_summary)));

        if !signal.details.is_empty() {
            lines.push(String::new());
            for (k, v) in &signal.details {
                lines.push(format!("• {}: <code>{}</code>", html_escape(k), html_escape(v)));
            }
        }

        lines.push(String::new());
        lines.push(format!(
            "🔗 <a href=\"{}\">เปิดกราฟ Binance Futures ({})</a>",
            signal.binance_futures_url(),
            html_escape(&signal.symbol)
        ));

        let text = lines.join("\n");
        let url = format!("https://api.telegram.org/bot{}/sendMessage", token);

        let payload = json!({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": true
        });

        let resp = self.http_client.post(&url).json(&payload).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Telegram API returned {}: {}", status, body);
        }

        Ok(())
    }

    async fn send_discord(&self, signal: &ScanSignal) -> Result<()> {
        let webhook_url = match &self.config.discord_webhook_url {
            Some(u) => u,
            None => return Ok(()),
        };

        let mut fields = vec![
            json!({
                "name": "🪙 คู่เหรียญ (Symbol)",
                "value": format!("**{}** (`{}`)", signal.symbol, signal.timeframe),
                "inline": true
            }),
            json!({
                "name": "💵 ราคา (Price)",
                "value": format!("`${}`", format_price(signal.price)),
                "inline": true
            }),
            json!({
                "name": "📌 สัญญาณ (Signal)",
                "value": format!("**{}**", signal.metric_summary),
                "inline": false
            }),
        ];

        if let Some(chg24) = signal.change_24h_pct {
            let sign = if chg24 >= 0.0 { "+" } else { "" };
            fields.push(json!({
                "name": "📊 24h Change",
                "value": format!("`{}{:.2}%`", sign, chg24),
                "inline": true
            }));
        }

        if let Some(vol24) = signal.quote_volume_24h {
            fields.push(json!({
                "name": "💧 24h Volume",
                "value": format!("`${}`", format_compact_usd(vol24)),
                "inline": true
            }));
        }

        for (k, v) in &signal.details {
            fields.push(json!({
                "name": k,
                "value": format!("`{}`", v),
                "inline": true
            }));
        }

        let payload = json!({
            "username": "Binance Futures Scanner (Rust)",
            "embeds": [{
                "title": format!("{} — {}", signal.symbol, signal.category.title_th()),
                "url": signal.binance_futures_url(),
                "color": signal.discord_color(),
                "fields": fields,
                "footer": {
                    "text": "Scanner-f • Binance USDT-M Perpetual Futures"
                },
                "timestamp": signal.timestamp.to_rfc3339()
            }]
        });

        let resp = self.http_client.post(webhook_url).json(&payload).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Discord Webhook returned {}: {}", status, body);
        }

        Ok(())
    }
}

pub fn format_price(price: f64) -> String {
    if price >= 1000.0 {
        format!("{:.2}", price)
    } else if price >= 1.0 {
        format!("{:.4}", price)
    } else if price >= 0.01 {
        format!("{:.5}", price)
    } else {
        format!("{:.7}", price)
    }
}

pub fn format_compact_usd(amount: f64) -> String {
    let abs = amount.abs();
    if abs >= 1_000_000_000.0 {
        format!("{:.2}B", amount / 1_000_000_000.0)
    } else if abs >= 1_000_000.0 {
        format!("{:.2}M", amount / 1_000_000.0)
    } else if abs >= 1_000.0 {
        format!("{:.2}K", amount / 1_000.0)
    } else {
        format!("{:.2}", amount)
    }
}

fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
