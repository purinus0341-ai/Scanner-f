use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::config::ScannerConfig;
use crate::models::{ScanSignal, SignalCategory};

#[derive(Clone)]
pub struct AlertNotifier {
    config: ScannerConfig,
    http_client: Client,
    long_poll_client: Client,
    cooldowns: Arc<Mutex<HashMap<String, i64>>>,
}

impl AlertNotifier {
    pub fn new(config: ScannerConfig) -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(12))
            .build()
            .expect("Failed to create HTTP client for AlertNotifier");
        let long_poll_client = Client::builder()
            .timeout(Duration::from_secs(35))
            .build()
            .expect("Failed to create long-poll HTTP client for AlertNotifier");

        Self {
            config,
            http_client,
            long_poll_client,
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

    /// Checks whether `(symbol, category)` is currently inside the cooldown window without mutating state.
    pub async fn is_in_cooldown(&self, symbol: &str, category: SignalCategory) -> bool {
        let cooldown_secs = self.config.alert_cooldown_secs as i64;
        let key = format!("{}:{}", symbol, category.as_key());
        let now = Utc::now().timestamp();
        let map = self.cooldowns.lock().await;
        if let Some(&last_sent) = map.get(&key) {
            return now - last_sent < cooldown_secs;
        }
        false
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

    /// Sends a formatted paper trading event notification to Telegram / Discord if enabled.
    pub async fn send_paper_trade_alert(
        &self,
        title: &str,
        symbol: &str,
        lines_th: &[String],
        discord_color: u32,
    ) {
        if !self.config.paper_notify_webhooks {
            return;
        }

        if let (Some(token), Some(chat_id)) = (
            &self.config.telegram_bot_token,
            &self.config.telegram_chat_id,
        ) {
            let mut msg_lines = Vec::with_capacity(lines_th.len() + 2);
            msg_lines.push(format!("<b>{}</b>", html_escape(title)));
            for line in lines_th {
                msg_lines.push(html_escape(line));
            }
            msg_lines.push(format!(
                "\n🔗 <a href=\"https://www.binance.com/en/futures/{}\">ดูกราฟ {}</a>",
                html_escape(symbol),
                html_escape(symbol)
            ));

            let mut payload = json!({
                "chat_id": chat_id,
                "text": msg_lines.join("\n"),
                "parse_mode": "HTML",
                "disable_web_page_preview": true
            });
            if self.config.paper_telegram_bot_enabled {
                payload["reply_markup"] = json!({
                    "inline_keyboard": [[
                        { "text": "💼 ดูพอร์ต + ปุ่มสั่งการ", "callback_data": "paper:panel" },
                        { "text": "📜 ประวัติ 10 รายการ", "callback_data": "paper:history_new" }
                    ]]
                });
            }

            let url = format!("https://api.telegram.org/bot{}/sendMessage", token);
            if let Err(err) = self.http_client.post(&url).json(&payload).send().await {
                error!("Failed to send Telegram paper alert: {:#}", err);
            }
        }

        if let Some(webhook_url) = &self.config.discord_webhook_url {
            let payload = json!({
                "username": "Scanner-f Paper Trader",
                "embeds": [{
                    "title": title,
                    "url": format!("https://www.binance.com/en/futures/{}", symbol),
                    "description": lines_th.join("\n"),
                    "color": discord_color,
                    "footer": { "text": "Scanner-f Paper Trading Simulator" },
                    "timestamp": Utc::now().to_rfc3339()
                }]
            });
            if let Err(err) = self.http_client.post(webhook_url).json(&payload).send().await {
                error!("Failed to send Discord paper alert: {:#}", err);
            }
        }
    }

    pub async fn send_telegram_html(
        &self,
        html_text: &str,
        inline_keyboard: Option<Value>,
    ) -> Result<i64> {
        let token = self
            .config
            .telegram_bot_token
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("TELEGRAM_BOT_TOKEN not configured"))?;
        let chat_id = self
            .config
            .telegram_chat_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("TELEGRAM_CHAT_ID not configured"))?;

        let mut payload = json!({
            "chat_id": chat_id,
            "text": html_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": true
        });
        if let Some(kb) = inline_keyboard {
            payload["reply_markup"] = json!({ "inline_keyboard": kb });
        }

        let url = format!("https://api.telegram.org/bot{}/sendMessage", token);
        let resp = self.http_client.post(&url).json(&payload).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Telegram sendMessage failed {}: {}", status, body);
        }
        let body: Value = resp.json().await.unwrap_or(json!({}));
        let msg_id = body
            .pointer("/result/message_id")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        Ok(msg_id)
    }

    pub async fn edit_telegram_html(
        &self,
        chat_id: &str,
        message_id: i64,
        html_text: &str,
        inline_keyboard: Option<Value>,
    ) -> Result<()> {
        let token = self
            .config
            .telegram_bot_token
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("TELEGRAM_BOT_TOKEN not configured"))?;

        let mut payload = json!({
            "chat_id": chat_id,
            "message_id": message_id,
            "text": html_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": true
        });
        if let Some(kb) = inline_keyboard {
            payload["reply_markup"] = json!({ "inline_keyboard": kb });
        }

        let url = format!("https://api.telegram.org/bot{}/editMessageText", token);
        let resp = self.http_client.post(&url).json(&payload).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if body.contains("message is not modified") {
                return Ok(());
            }
            anyhow::bail!("Telegram editMessageText failed {}: {}", status, body);
        }
        Ok(())
    }

    pub async fn answer_telegram_callback(
        &self,
        callback_query_id: &str,
        text: Option<&str>,
        show_alert: bool,
    ) {
        let Some(token) = &self.config.telegram_bot_token else {
            return;
        };
        let mut payload = json!({
            "callback_query_id": callback_query_id,
            "show_alert": show_alert
        });
        if let Some(t) = text {
            payload["text"] = Value::String(t.to_string());
        }
        let url = format!("https://api.telegram.org/bot{}/answerCallbackQuery", token);
        let _ = self.http_client.post(&url).json(&payload).send().await;
    }

    pub async fn set_telegram_commands(&self) {
        let Some(token) = &self.config.telegram_bot_token else {
            return;
        };
        let payload = json!({
            "commands": [
                { "command": "portfolio", "description": "💼 ดูพอร์ตจำลอง + ปุ่มสั่งการ" },
                { "command": "history", "description": "📜 ดูประวัติเทรด 10 รายการล่าสุด" },
                { "command": "scan", "description": "🔍 สแกนตลาด MTF ทันที 1 รอบ" },
                { "command": "pause", "description": "⏸️ พักการเปิดออเดอร์ใหม่ชั่วคราว" },
                { "command": "resume", "description": "▶️ เปิดรับออเดอร์จำลองใหม่อีกครั้ง" },
                { "command": "closeall", "description": "❌ ปิดออเดอร์จำลองทั้งหมดทันที" },
                { "command": "reset", "description": "🔄 รีเซ็ตพอร์ตจำลองกลับเป็นทุนตั้งต้น" },
                { "command": "help", "description": "❓ ดูคำสั่งทั้งหมดของบอท" }
            ]
        });
        let url = format!("https://api.telegram.org/bot{}/setMyCommands", token);
        let _ = self.http_client.post(&url).json(&payload).send().await;
    }

    pub async fn get_telegram_updates(
        &self,
        offset: Option<i64>,
        timeout_secs: u64,
    ) -> Result<Vec<Value>> {
        let token = self
            .config
            .telegram_bot_token
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("TELEGRAM_BOT_TOKEN not configured"))?;
        let url = format!("https://api.telegram.org/bot{}/getUpdates", token);
        let mut payload = json!({
            "timeout": timeout_secs,
            "allowed_updates": ["message", "callback_query"]
        });
        if let Some(off) = offset {
            payload["offset"] = json!(off);
        }

        let resp = self.long_poll_client.post(&url).json(&payload).send().await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(json!({}));
        if !status.is_success() || !body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            let desc = body
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            anyhow::bail!("Telegram getUpdates {} ({})", status, desc);
        }
        Ok(body
            .get("result")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default())
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

pub fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
