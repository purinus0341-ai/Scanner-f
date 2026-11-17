use std::sync::Arc;
use std::time::Duration;

use chrono::Local;
use serde_json::{json, Value};
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::binance::BinanceFuturesClient;
use crate::config::ScannerConfig;
use crate::notifier::{format_price, html_escape, AlertNotifier};
use crate::paper_trader::{CsvTradeRow, PaperPortfolioState, PaperSide, PaperTrader};
use crate::strategy::TradingStrategy;

#[derive(Clone)]
pub struct TelegramBotController {
    config: ScannerConfig,
    client: BinanceFuturesClient,
    notifier: AlertNotifier,
    paper_trader: PaperTrader,
    scan_now_notify: Arc<Notify>,
}

impl TelegramBotController {
    pub fn new(
        config: ScannerConfig,
        client: BinanceFuturesClient,
        notifier: AlertNotifier,
        paper_trader: PaperTrader,
        scan_now_notify: Arc<Notify>,
    ) -> Self {
        Self {
            config,
            client,
            notifier,
            paper_trader,
            scan_now_notify,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.paper_trading_enabled
            && self.config.paper_telegram_bot_enabled
            && self.config.has_telegram()
    }

    /// Sends a fresh interactive Portfolio Dashboard message to Telegram.
    pub async fn send_dashboard_message(&self, refresh_prices: bool, notice: Option<&str>) {
        if !self.is_enabled() {
            return;
        }
        if refresh_prices {
            if let Ok(prices) = self.client.fetch_all_prices().await {
                self.paper_trader.update_prices(&prices).await;
            }
        }
        let state = self.paper_trader.get_state_snapshot().await;
        let (html_text, kb) = render_dashboard_html(&state, &self.config, notice);
        if let Err(err) = self.notifier.send_telegram_html(&html_text, Some(kb)).await {
            warn!("ส่ง Telegram Portfolio Dashboard ไม่สำเร็จ: {:#}", err);
        }
    }

    pub fn spawn_background(self: Arc<Self>) {
        if !self.is_enabled() {
            return;
        }

        // Spawn periodic Telegram summary if configured
        if self.config.paper_telegram_summary_mins > 0 {
            let bot_periodic = self.clone();
            let mins = self.config.paper_telegram_summary_mins;
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(mins * 60));
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    bot_periodic
                        .send_dashboard_message(true, Some("⏰ สรุปพอร์ตจำลองตามรอบเวลา"))
                        .await;
                }
            });
        }

        tokio::spawn(async move {
            info!("🤖 เปิดใช้งาน Telegram Bot Listener สำหรับดูพอร์ตจำลองและปุ่มสั่งการเรียลไทม์");
            self.notifier.set_telegram_commands().await;

            // Drain any stale queued updates on startup so old clicks/commands don't replay unexpectedly
            let mut next_offset: Option<i64> = None;
            if let Ok(stale) = self.notifier.get_telegram_updates(Some(-1), 0).await {
                if let Some(max_id) = stale
                    .iter()
                    .filter_map(|u| u.get("update_id").and_then(|v| v.as_i64()))
                    .max()
                {
                    next_offset = Some(max_id + 1);
                }
            }

            loop {
                match self.notifier.get_telegram_updates(next_offset, 20).await {
                    Ok(updates) => {
                        for upd in updates {
                            if let Some(uid) = upd.get("update_id").and_then(|v| v.as_i64()) {
                                next_offset = Some(uid + 1);
                            }
                            self.handle_update(&upd).await;
                        }
                    }
                    Err(err) => {
                        let msg = format!("{:#}", err);
                        if msg.contains("409") || msg.contains("Conflict") {
                            warn!(
                                "พบ Telegram getUpdates ซ้อน (409 Conflict) — รอ 10 วินาทีแล้วลองใหม่"
                            );
                            tokio::time::sleep(Duration::from_secs(10)).await;
                        } else {
                            warn!("Telegram Bot Polling ขัดข้องชั่วคราว: {}", msg);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
            }
        });
    }

    fn is_authorized_chat(&self, chat_id: i64) -> bool {
        self.config
            .telegram_chat_id
            .as_deref()
            .map(|expected| expected.trim() == chat_id.to_string())
            .unwrap_or(false)
    }

    async fn handle_update(&self, upd: &Value) {
        if let Some(cb) = upd.get("callback_query") {
            let cb_id = cb.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let data = cb.get("data").and_then(|v| v.as_str()).unwrap_or("");
            let chat_id = cb
                .pointer("/message/chat/id")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let message_id = cb
                .pointer("/message/message_id")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            if !self.is_authorized_chat(chat_id) {
                self.notifier
                    .answer_telegram_callback(cb_id, Some("⛔ เฉพาะเจ้าของบอทเท่านั้น"), true)
                    .await;
                return;
            }
            self.handle_callback(cb_id, data, &chat_id.to_string(), message_id)
                .await;
            return;
        }

        if let Some(msg) = upd.get("message") {
            let chat_id = msg
                .pointer("/chat/id")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if !self.is_authorized_chat(chat_id) {
                return;
            }
            let text = msg.get("text").and_then(|v| v.as_str()).unwrap_or("").trim();
            if text.starts_with('/') {
                let mut parts = text.splitn(2, char::is_whitespace);
                let cmd = parts
                    .next()
                    .unwrap_or("")
                    .split('@')
                    .next()
                    .unwrap_or("")
                    .to_lowercase();
                let args = parts.next().unwrap_or("").trim();
                self.handle_command(&cmd, args).await;
            }
        }
    }

    async fn handle_command(&self, cmd: &str, args: &str) {
        match cmd {
            "/start" | "/portfolio" | "/status" | "/positions" => {
                self.send_dashboard_message(true, None).await;
            }
            "/strategy" | "/strat" | "/mode" => {
                if args.is_empty() {
                    let st = self.paper_trader.get_state_snapshot().await;
                    let (html_text, kb) = render_strategy_html(&st, &self.config, None);
                    let _ = self.notifier.send_telegram_html(&html_text, Some(kb)).await;
                } else if let Some(strat) = TradingStrategy::from_str_lossy(args) {
                    let st = self.paper_trader.set_strategy(strat).await;
                    let eff_stars = st.effective_min_stars(&self.config);
                    let notice = format!(
                        "🎯 เปลี่ยนกลยุทธ์เป็น {} แล้ว — ปรับเกณฑ์เป็น ≥{}⭐ อัตโนมัติ (คงค่า Lev/Size/Max เดิม)",
                        strat.title_th(),
                        eff_stars
                    );
                    let (html_text, kb) = render_strategy_html(&st, &self.config, Some(&notice));
                    let _ = self.notifier.send_telegram_html(&html_text, Some(kb)).await;
                } else {
                    let st = self.paper_trader.get_state_snapshot().await;
                    let notice = format!(
                        "⚠️ ไม่รู้จักกลยุทธ์ `{}` — เลือกได้: balanced, trend, breakout, reversal, scalp",
                        args
                    );
                    let (html_text, kb) = render_strategy_html(&st, &self.config, Some(&notice));
                    let _ = self.notifier.send_telegram_html(&html_text, Some(kb)).await;
                }
            }
            "/settings" | "/config" => {
                let st = self.paper_trader.get_state_snapshot().await;
                let (html_text, kb) = render_settings_html(&st, &self.config, None);
                let _ = self.notifier.send_telegram_html(&html_text, Some(kb)).await;
            }
            "/set" => {
                let (html_text, kb) = if args.is_empty() {
                    let st = self.paper_trader.get_state_snapshot().await;
                    render_settings_html(
                        &st,
                        &self.config,
                        Some("💡 พิมพ์ค่าที่ต้องการต่อท้าย /set เช่น /set max 8 lev 15 หรือ /set strategy trend หรือ /set default"),
                    )
                } else {
                    match parse_set_command_args(args) {
                        Ok(ParsedSetCommand::ResetToDefault) => {
                            let st = self.paper_trader.clear_runtime_settings().await;
                            render_settings_html(
                                &st,
                                &self.config,
                                Some("🔄 คืนค่าเริ่มต้นจากไฟล์ .env เรียบร้อยแล้ว"),
                            )
                        }
                        Ok(ParsedSetCommand::Update {
                            max_open_positions,
                            position_pct,
                            leverage,
                            min_stars,
                            strategy,
                        }) => {
                            if let Some(strat) = strategy {
                                let _ = self.paper_trader.set_strategy(strat).await;
                            }
                            let st = if max_open_positions.is_some()
                                || position_pct.is_some()
                                || leverage.is_some()
                                || min_stars.is_some()
                            {
                                self.paper_trader
                                    .update_runtime_settings(
                                        max_open_positions,
                                        position_pct,
                                        leverage,
                                        min_stars,
                                    )
                                    .await
                            } else {
                                self.paper_trader.get_state_snapshot().await
                            };
                            let eff_strat = st.effective_strategy(&self.config);
                            let notice = format!(
                                "✅ บันทึกค่าใหม่แล้ว: {} | Max {} ไม้ | ไม้ละ {}% | {}x | ≥ {}⭐",
                                eff_strat.short_tag(),
                                st.effective_max_open_positions(&self.config),
                                fmt_num_clean(st.effective_position_pct(&self.config)),
                                fmt_num_clean(st.effective_leverage(&self.config)),
                                st.effective_min_stars(&self.config)
                            );
                            render_settings_html(&st, &self.config, Some(&notice))
                        }
                        Err(err_msg) => {
                            let st = self.paper_trader.get_state_snapshot().await;
                            let notice = format!("⚠️ {}", err_msg);
                            render_settings_html(&st, &self.config, Some(&notice))
                        }
                    }
                };
                let _ = self.notifier.send_telegram_html(&html_text, Some(kb)).await;
            }
            "/history" => {
                let rows = self.paper_trader.recent_csv_events(10);
                let (html_text, kb) = render_history_html(&rows);
                let _ = self.notifier.send_telegram_html(&html_text, Some(kb)).await;
            }
            "/pause" => {
                let st = self.paper_trader.set_paused(true).await;
                let (html_text, kb) = render_dashboard_html(
                    &st,
                    &self.config,
                    Some("⏸️ พักการเปิดออเดอร์ใหม่แล้ว (ออเดอร์ที่ถืออยู่ยังรัน TP/SL ต่อ)"),
                );
                let _ = self.notifier.send_telegram_html(&html_text, Some(kb)).await;
            }
            "/resume" => {
                let st = self.paper_trader.set_paused(false).await;
                let (html_text, kb) = render_dashboard_html(
                    &st,
                    &self.config,
                    Some("▶️ เปิดรับออเดอร์จำลองใหม่อีกครั้งแล้ว"),
                );
                let _ = self.notifier.send_telegram_html(&html_text, Some(kb)).await;
            }
            "/scan" => {
                let _ = self
                    .notifier
                    .send_telegram_html("🔍 กำลังเริ่มสแกนตลาดและอัปเดตราคาทันที...", None)
                    .await;
                self.scan_now_notify.notify_one();
            }
            "/closeall" => {
                let st = self.paper_trader.get_state_snapshot().await;
                let (html_text, kb) = render_close_all_confirm_html(&st);
                let _ = self.notifier.send_telegram_html(&html_text, Some(kb)).await;
            }
            "/reset" => {
                let (html_text, kb) = render_reset_confirm_html(&self.config);
                let _ = self.notifier.send_telegram_html(&html_text, Some(kb)).await;
            }
            "/help" => {
                let help_text = concat!(
                    "🤖 <b>คำสั่งควบคุมพอร์ตจำลอง (Scanner-f)</b>\n",
                    "• /portfolio — ดูสรุปพอร์ตจำลอง + ปุ่มสั่งการ\n",
                    "• /strategy — เลือกกลยุทธ์ 5 แบบ + ดูสถิติ Win Rate/PnL แยกสาย\n",
                    "• /settings — เปิดหน้าตั้งค่า Max ไม้ / ขนาด% / Leverage / ดาว\n",
                    "• /set — ปรับค่าเอง เช่น <code>/set max 8 lev 15</code> หรือ <code>/set strategy trend</code> หรือ <code>/set default</code>\n",
                    "• /history — ดูประวัติการเทรด 10 รายการล่าสุด\n",
                    "• /scan — สแกนตลาดและอัปเดตราคาทันที\n",
                    "• /pause — พักการเปิดออเดอร์ใหม่ชั่วคราว\n",
                    "• /resume — เปิดรับออเดอร์ใหม่อีกครั้ง\n",
                    "• /closeall — ปิดออเดอร์จำลองทั้งหมดทันที (มีหน้ายืนยัน)\n",
                    "• /reset — รีเซ็ตพอร์ตจำลองกลับเป็นทุนเริ่มต้น (มีหน้ายืนยัน)"
                );
                let kb = json!([
                    [
                        { "text": "💼 เปิดพอร์ตจำลอง", "callback_data": "paper:panel" },
                        { "text": "🎯 เลือกกลยุทธ์", "callback_data": "paper:strategy" },
                        { "text": "⚙️ ตั้งค่าพอร์ต", "callback_data": "paper:settings" }
                    ]
                ]);
                let _ = self.notifier.send_telegram_html(help_text, Some(kb)).await;
            }
            _ => {}
        }
    }

    async fn handle_callback(&self, cb_id: &str, data: &str, chat_id: &str, msg_id: i64) {
        match data {
            "paper:panel" => {
                self.notifier
                    .answer_telegram_callback(cb_id, Some("💼 กำลังเปิดหน้าพอร์ตล่าสุด..."), false)
                    .await;
                self.send_dashboard_message(true, None).await;
            }
            "paper:refresh" => {
                self.notifier
                    .answer_telegram_callback(cb_id, Some("🔄 อัปเดตราคาและพอร์ตล่าสุดแล้ว"), false)
                    .await;
                if let Ok(prices) = self.client.fetch_all_prices().await {
                    self.paper_trader.update_prices(&prices).await;
                }
                let st = self.paper_trader.get_state_snapshot().await;
                let (html_text, kb) = render_dashboard_html(&st, &self.config, None);
                let _ = self
                    .notifier
                    .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                    .await;
            }
            "paper:strategy" => {
                self.notifier
                    .answer_telegram_callback(cb_id, Some("🎯 เปิดหน้าเลือกกลยุทธ์ + สถิติแยกสาย"), false)
                    .await;
                let st = self.paper_trader.get_state_snapshot().await;
                let (html_text, kb) = render_strategy_html(&st, &self.config, None);
                let _ = self
                    .notifier
                    .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                    .await;
            }
            other if other.starts_with("paper:strat:") => {
                let raw_strat = other.trim_start_matches("paper:strat:");
                if let Some(strat) = TradingStrategy::from_str_lossy(raw_strat) {
                    let st = self.paper_trader.set_strategy(strat).await;
                    let eff_stars = st.effective_min_stars(&self.config);
                    let toast = format!("🎯 ใช้กลยุทธ์ {} (≥{}⭐)", strat.title_th(), eff_stars);
                    let notice = format!(
                        "🎯 เปลี่ยนกลยุทธ์เป็น {} แล้ว — ปรับเกณฑ์เป็น ≥{}⭐ อัตโนมัติ (มีผลตั้งแต่สัญญาณถัดไป; คงค่า Lev/Size/Max เดิม)",
                        strat.title_th(),
                        eff_stars
                    );
                    self.notifier
                        .answer_telegram_callback(cb_id, Some(&toast), false)
                        .await;
                    let (html_text, kb) = render_strategy_html(&st, &self.config, Some(&notice));
                    let _ = self
                        .notifier
                        .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                        .await;
                } else {
                    self.notifier
                        .answer_telegram_callback(cb_id, Some("⚠️ ไม่พบกลยุทธ์ที่เลือก"), true)
                        .await;
                }
            }
            "paper:settings" => {
                self.notifier
                    .answer_telegram_callback(cb_id, Some("⚙️ เปิดหน้าตั้งค่าพอร์ตจำลอง"), false)
                    .await;
                let st = self.paper_trader.get_state_snapshot().await;
                let (html_text, kb) = render_settings_html(&st, &self.config, None);
                let _ = self
                    .notifier
                    .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                    .await;
            }
            "paper:cfg_default" => {
                let st = self.paper_trader.clear_runtime_settings().await;
                self.notifier
                    .answer_telegram_callback(cb_id, Some("🔄 คืนค่าเริ่มต้นจาก .env แล้ว"), false)
                    .await;
                let (html_text, kb) = render_settings_html(
                    &st,
                    &self.config,
                    Some("🔄 คืนค่าเริ่มต้นจากไฟล์ .env เรียบร้อยแล้ว"),
                );
                let _ = self
                    .notifier
                    .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                    .await;
            }
            other if other.starts_with("paper:set:") => {
                let rest = other.trim_start_matches("paper:set:");
                let mut parts = rest.splitn(2, ':');
                let field = parts.next().unwrap_or("");
                let raw_val = parts.next().unwrap_or("");
                let mut updated: Option<(PaperPortfolioState, String, String)> = None;

                match field {
                    "max" => {
                        if let Ok(v) = raw_val.parse::<usize>() {
                            let st = self
                                .paper_trader
                                .update_runtime_settings(Some(v), None, None, None)
                                .await;
                            let eff = st.effective_max_open_positions(&self.config);
                            updated = Some((
                                st,
                                format!("✅ ตั้งจำนวนไม้สูงสุดเป็น {} ไม้แล้ว", eff),
                                format!("✅ ปรับจำนวนไม้สูงสุดเป็น {} ไม้แล้ว (มีผลทันทีตั้งแต่ไม้ถัดไป)", eff),
                            ));
                        }
                    }
                    "size" => {
                        if let Ok(v) = raw_val.parse::<f64>() {
                            let st = self
                                .paper_trader
                                .update_runtime_settings(None, Some(v), None, None)
                                .await;
                            let eff = st.effective_position_pct(&self.config);
                            updated = Some((
                                st,
                                format!("✅ ตั้งขนาดไม้เป็น {:.0}% แล้ว", eff),
                                format!("✅ ปรับขนาดต่อไม้เป็น {:.0}% แล้ว (มีผลทันทีตั้งแต่ไม้ถัดไป)", eff),
                            ));
                        }
                    }
                    "lev" => {
                        if let Ok(v) = raw_val.parse::<f64>() {
                            let st = self
                                .paper_trader
                                .update_runtime_settings(None, None, Some(v), None)
                                .await;
                            let eff = st.effective_leverage(&self.config);
                            updated = Some((
                                st,
                                format!("✅ ตั้ง Leverage เป็น {:.0}x แล้ว", eff),
                                format!("✅ ปรับ Leverage เป็น {:.0}x แล้ว (มีผลทันทีตั้งแต่ไม้ถัดไป)", eff),
                            ));
                        }
                    }
                    "stars" => {
                        if let Ok(v) = raw_val.parse::<u8>() {
                            let st = self
                                .paper_trader
                                .update_runtime_settings(None, None, None, Some(v))
                                .await;
                            let eff = st.effective_min_stars(&self.config);
                            let label = if eff >= 3 {
                                "เฉพาะ 3⭐ A+".to_string()
                            } else {
                                format!("≥ {}⭐ ขึ้นไป", eff)
                            };
                            updated = Some((
                                st,
                                format!("✅ ตั้งเกณฑ์เข้าสัญญาณเป็น {} แล้ว", label),
                                format!("✅ ปรับเกณฑ์รับสัญญาณเป็น {} แล้ว", label),
                            ));
                        }
                    }
                    _ => {}
                }

                if let Some((st, toast, notice)) = updated {
                    self.notifier
                        .answer_telegram_callback(cb_id, Some(&toast), false)
                        .await;
                    let (html_text, kb) = render_settings_html(&st, &self.config, Some(&notice));
                    let _ = self
                        .notifier
                        .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                        .await;
                } else {
                    self.notifier
                        .answer_telegram_callback(cb_id, Some("⚠️ ค่าตั้งค่าไม่ถูกต้อง"), true)
                        .await;
                }
            }
            "paper:history" => {
                self.notifier
                    .answer_telegram_callback(cb_id, Some("📜 โหลดประวัติเทรดล่าสุดแล้ว"), false)
                    .await;
                let rows = self.paper_trader.recent_csv_events(10);
                let (html_text, kb) = render_history_html(&rows);
                let _ = self
                    .notifier
                    .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                    .await;
            }
            "paper:history_new" => {
                self.notifier
                    .answer_telegram_callback(cb_id, Some("📜 เปิดประวัติเทรดล่าสุดแล้ว"), false)
                    .await;
                let rows = self.paper_trader.recent_csv_events(10);
                let (html_text, kb) = render_history_html(&rows);
                let _ = self.notifier.send_telegram_html(&html_text, Some(kb)).await;
            }
            "paper:pause" => {
                self.notifier
                    .answer_telegram_callback(cb_id, Some("⏸️ พักการเปิดไม้ใหม่แล้ว"), false)
                    .await;
                let st = self.paper_trader.set_paused(true).await;
                let (html_text, kb) = render_dashboard_html(
                    &st,
                    &self.config,
                    Some("⏸️ พักการเปิดออเดอร์ใหม่ชั่วคราว (ออเดอร์เดิมยังดูแล TP/SL ตามปกติ)"),
                );
                let _ = self
                    .notifier
                    .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                    .await;
            }
            "paper:resume" => {
                self.notifier
                    .answer_telegram_callback(cb_id, Some("▶️ เปิดรับออเดอร์ใหม่แล้ว"), false)
                    .await;
                let st = self.paper_trader.set_paused(false).await;
                let (html_text, kb) = render_dashboard_html(
                    &st,
                    &self.config,
                    Some("▶️ เปิดรับออเดอร์จำลองจากสัญญาณตามเกณฑ์ที่ตั้งไว้แล้ว"),
                );
                let _ = self
                    .notifier
                    .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                    .await;
            }
            "paper:scan" => {
                self.notifier
                    .answer_telegram_callback(cb_id, Some("🔍 กำลังรีเฟรชและสแกนตลาดทันที!"), false)
                    .await;
                self.scan_now_notify.notify_one();
                if let Ok(prices) = self.client.fetch_all_prices().await {
                    self.paper_trader.update_prices(&prices).await;
                }
                let st = self.paper_trader.get_state_snapshot().await;
                let (html_text, kb) = render_dashboard_html(
                    &st,
                    &self.config,
                    Some("🔍 สั่งรีเฟรชข้อมูลตลาดและ Funding/OI เรียบร้อยแล้ว"),
                );
                let _ = self
                    .notifier
                    .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                    .await;
            }
            "paper:manage" => {
                self.notifier
                    .answer_telegram_callback(cb_id, Some("📋 เลือกออเดอร์ที่ต้องการปิด"), false)
                    .await;
                if let Ok(prices) = self.client.fetch_all_prices().await {
                    self.paper_trader.update_prices(&prices).await;
                }
                let st = self.paper_trader.get_state_snapshot().await;
                let (html_text, kb) = render_manage_html(&st);
                let _ = self
                    .notifier
                    .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                    .await;
            }
            "paper:close_all_prompt" => {
                self.notifier.answer_telegram_callback(cb_id, None, false).await;
                let st = self.paper_trader.get_state_snapshot().await;
                let (html_text, kb) = render_close_all_confirm_html(&st);
                let _ = self
                    .notifier
                    .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                    .await;
            }
            "paper:close_all_confirm" => {
                let prices = self.client.fetch_all_prices().await.ok();
                let (cnt, sum_pnl) = self.paper_trader.close_all_positions(prices.as_ref()).await;
                let toast = format!("❌ ปิดทั้งหมด {} ไม้ (สุทธิ {:+.2} USDT)", cnt, sum_pnl);
                self.notifier
                    .answer_telegram_callback(cb_id, Some(&toast), false)
                    .await;
                let st = self.paper_trader.get_state_snapshot().await;
                let notice = format!("❌ ปิดออเดอร์ด้วยตนเอง {} ไม้ (PnL รวม {:+.2} USDT)", cnt, sum_pnl);
                let (html_text, kb) = render_dashboard_html(&st, &self.config, Some(&notice));
                let _ = self
                    .notifier
                    .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                    .await;
            }
            "paper:reset_prompt" => {
                self.notifier.answer_telegram_callback(cb_id, None, false).await;
                let (html_text, kb) = render_reset_confirm_html(&self.config);
                let _ = self
                    .notifier
                    .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                    .await;
            }
            "paper:reset_confirm" => {
                if let Ok(st) = self.paper_trader.reset_portfolio_async().await {
                    self.notifier
                        .answer_telegram_callback(
                            cb_id,
                            Some("🔄 รีเซ็ตพอร์ตจำลองกลับเป็นทุนเริ่มต้นเรียบร้อย!"),
                            false,
                        )
                        .await;
                    let (html_text, kb) = render_dashboard_html(
                        &st,
                        &self.config,
                        Some("🔄 รีเซ็ตพอร์ตและล้างประวัติเรียบร้อยแล้ว (คงค่าตั้งขนาดไม้/Lev ที่ปรับไว้)"),
                    );
                    let _ = self
                        .notifier
                        .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                        .await;
                } else {
                    self.notifier
                        .answer_telegram_callback(cb_id, Some("⚠️ รีเซ็ตไม่สำเร็จ"), true)
                        .await;
                }
            }
            other if other.starts_with("paper:close:") => {
                if let Ok(tid) = other.trim_start_matches("paper:close:").parse::<u64>() {
                    let prices = self.client.fetch_all_prices().await.ok();
                    if let Some((sym, pnl)) = self
                        .paper_trader
                        .close_position_by_id(tid, prices.as_ref())
                        .await
                    {
                        let toast = format!("✅ ปิด #{} {} ({:+.2} USDT)", tid, sym, pnl);
                        self.notifier
                            .answer_telegram_callback(cb_id, Some(&toast), false)
                            .await;
                        let st = self.paper_trader.get_state_snapshot().await;
                        let notice = format!("✅ ปิดไม้ #{} {} แล้ว ({:+.2} USDT)", tid, sym, pnl);
                        let (html_text, kb) = render_dashboard_html(&st, &self.config, Some(&notice));
                        let _ = self
                            .notifier
                            .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                            .await;
                    } else {
                        self.notifier
                            .answer_telegram_callback(cb_id, Some("⚠️ ไม่พบไม้นี้ (อาจปิดไปแล้ว)"), false)
                            .await;
                        let st = self.paper_trader.get_state_snapshot().await;
                        let (html_text, kb) = render_dashboard_html(&st, &self.config, None);
                        let _ = self
                            .notifier
                            .edit_telegram_html(chat_id, msg_id, &html_text, Some(kb))
                            .await;
                    }
                }
            }
            _ => {
                self.notifier.answer_telegram_callback(cb_id, None, false).await;
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedSetCommand {
    ResetToDefault,
    Update {
        max_open_positions: Option<usize>,
        position_pct: Option<f64>,
        leverage: Option<f64>,
        min_stars: Option<u8>,
        strategy: Option<TradingStrategy>,
    },
}

pub fn parse_set_command_args(args: &str) -> Result<ParsedSetCommand, String> {
    let mut tokens: Vec<String> = Vec::new();
    for raw in args.split_whitespace() {
        if let Some((k, v)) = raw.split_once('=') {
            if !k.is_empty() && !v.is_empty() {
                tokens.push(k.to_string());
                tokens.push(v.to_string());
                continue;
            }
        } else if let Some((k, v)) = raw.split_once(':') {
            if !k.is_empty() && !v.is_empty() {
                tokens.push(k.to_string());
                tokens.push(v.to_string());
                continue;
            }
        }
        tokens.push(raw.to_string());
    }

    if tokens.is_empty() {
        return Err(
            "กรุณาระบุค่าที่ต้องการ เช่น /set lev 15 หรือ /set strategy trend".to_string(),
        );
    }

    if tokens.len() == 1 {
        let single = tokens[0].trim().to_lowercase();
        if matches!(single.as_str(), "default" | "reset" | ".env" | "ค่าเริ่มต้น") {
            return Ok(ParsedSetCommand::ResetToDefault);
        }
    }

    let mut max_open_positions: Option<usize> = None;
    let mut position_pct: Option<f64> = None;
    let mut leverage: Option<f64> = None;
    let mut min_stars: Option<u8> = None;
    let mut strategy: Option<TradingStrategy> = None;
    let mut updated_any = false;

    let mut i = 0;
    while i < tokens.len() {
        let raw_key = tokens[i].trim();
        let key = raw_key.trim_start_matches('-').to_lowercase();
        if matches!(key.as_str(), "default" | "reset" | ".env" | "ค่าเริ่มต้น") {
            return Ok(ParsedSetCommand::ResetToDefault);
        }
        if i + 1 >= tokens.len() {
            return Err(format!(
                "ยังไม่ได้ใส่ค่าหลัง `{}` (เช่น /set strategy trend หรือ /set lev 15)",
                raw_key
            ));
        }
        let raw_val = tokens[i + 1].trim();
        match key.as_str() {
            "strategy" | "strat" | "mode" | "profile" | "กลยุทธ์" | "สาย" => {
                let parsed = TradingStrategy::from_str_lossy(raw_val).ok_or_else(|| {
                    format!(
                        "ไม่รู้จักกลยุทธ์ `{}` — เลือกได้: balanced, trend, breakout, reversal, scalp",
                        raw_val
                    )
                })?;
                strategy = Some(parsed);
                updated_any = true;
            }
            "max" | "maxpos" | "max_pos" | "max_positions" | "positions" | "pos" | "limit"
            | "ไม้" => {
                let cleaned = raw_val.trim_end_matches("ไม้").trim();
                let parsed = cleaned.parse::<i64>().map_err(|_| {
                    format!("จำนวนไม้ `{}` ไม่ถูกต้อง (ต้องเป็นเลข 1–30 เช่น /set max 8)", raw_val)
                })?;
                if parsed <= 0 {
                    return Err("จำนวนไม้สูงสุดต้องมากกว่า 0 (1–30 ไม้)".to_string());
                }
                max_open_positions = Some((parsed as usize).clamp(1, 30));
                updated_any = true;
            }
            "size" | "pct" | "margin" | "pos_pct" | "position_pct" | "percent" | "ขนาด" => {
                let cleaned = raw_val.trim_end_matches('%').trim();
                let parsed = cleaned.parse::<f64>().map_err(|_| {
                    format!("ขนาดไม้ `{}` ไม่ถูกต้อง (ต้องเป็นเลข 1–100 เช่น /set size 12)", raw_val)
                })?;
                if !parsed.is_finite() || parsed <= 0.0 {
                    return Err("ขนาดต่อไม้ (%) ต้องมากกว่า 0 (1–100%)".to_string());
                }
                position_pct = Some(parsed.clamp(1.0, 100.0));
                updated_any = true;
            }
            "lev" | "leverage" | "x" | "คูณ" => {
                let cleaned = raw_val.trim_end_matches(['x', 'X']).trim();
                let parsed = cleaned.parse::<f64>().map_err(|_| {
                    format!("Leverage `{}` ไม่ถูกต้อง (ต้องเป็นเลข 1–125 เช่น /set lev 15)", raw_val)
                })?;
                if !parsed.is_finite() || parsed <= 0.0 {
                    return Err("Leverage ต้องมากกว่า 0 (1–125x)".to_string());
                }
                leverage = Some(parsed.clamp(1.0, 125.0));
                updated_any = true;
            }
            "stars" | "star" | "min_stars" | "minstars" | "ดาว" => {
                let cleaned = raw_val
                    .trim_end_matches("ดาว")
                    .trim_end_matches('⭐')
                    .trim_end_matches('*')
                    .trim();
                let parsed = cleaned.parse::<i64>().map_err(|_| {
                    format!("ระดับดาว `{}` ไม่ถูกต้อง (เลือก 2 หรือ 3 เช่น /set stars 3)", raw_val)
                })?;
                if parsed <= 0 {
                    return Err("ระดับดาวขั้นต่ำต้องเป็น 1–3 ดาว".to_string());
                }
                min_stars = Some((parsed as u8).clamp(1, 3));
                updated_any = true;
            }
            _ => {
                return Err(format!(
                    "ไม่รู้จักหัวข้อ `{}` — ใช้ strategy, max, size, lev, stars หรือ default",
                    raw_key
                ));
            }
        }
        i += 2;
    }

    if !updated_any {
        return Err("กรุณาระบุค่าที่ต้องการตั้งอย่างน้อย 1 รายการ".to_string());
    }

    Ok(ParsedSetCommand::Update {
        max_open_positions,
        position_pct,
        leverage,
        min_stars,
        strategy,
    })
}

fn fmt_num_clean(v: f64) -> String {
    if (v - v.round()).abs() < 1e-6 {
        format!("{:.0}", v.round())
    } else {
        let s = format!("{:.2}", v);
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    }
}

pub fn render_dashboard_html(
    state: &PaperPortfolioState,
    cfg: &ScannerConfig,
    notice_line: Option<&str>,
) -> (String, Value) {
    let eq = state.total_equity();
    let net_usdt = eq - state.initial_balance;
    let net_pct = if state.initial_balance > 0.0 {
        (net_usdt / state.initial_balance) * 100.0
    } else {
        0.0
    };
    let status_badge = if state.paused_opening {
        "⏸️ พักเปิดไม้ใหม่ชั่วคราว"
    } else {
        "🟢 เปิดรับสัญญาณอัตโนมัติ"
    };

    let eff_strat = state.effective_strategy(cfg);
    let eff_lev = state.effective_leverage(cfg);
    let eff_pct = state.effective_position_pct(cfg);
    let eff_max = state.effective_max_open_positions(cfg);
    let eff_stars = state.effective_min_stars(cfg);
    let custom_badge = if state.has_custom_settings() {
        " [ปรับเอง]"
    } else {
        ""
    };
    let strat_lock = if eff_strat.requires_1h_alignment() {
        " [1h Lock]"
    } else {
        ""
    };

    let mut lines = Vec::new();
    if let Some(note) = notice_line {
        lines.push(format!("ℹ️ <i>{}</i>", html_escape(note)));
        lines.push(String::new());
    }
    lines.push("💼 <b>พอร์ตจำลองการเทรด (Paper Trading Dashboard)</b>".to_string());
    lines.push(format!("• สถานะเปิดไม้ใหม่: <code>{}</code>", status_badge));
    lines.push(format!(
        "• กลยุทธ์ที่ใช้งาน: <code>{}{}</code>",
        html_escape(eff_strat.title_th()),
        strat_lock
    ));
    lines.push(format!(
        "• ตั้งค่าไม้: <code>{}x | ไม้ละ {}% | สูงสุด {} ไม้ | ≥ {}⭐{}</code>",
        fmt_num_clean(eff_lev),
        fmt_num_clean(eff_pct),
        eff_max,
        eff_stars,
        custom_badge
    ));
    lines.push(format!(
        "• มูลค่าพอร์ต (Equity): <code>${:.2}</code> (<code>{:+.2} USDT | {:+.2}%</code>)",
        eq, net_usdt, net_pct
    ));
    lines.push(format!(
        "• ทุนตั้งต้น: <code>${:.2}</code> | เงินว่าง: <code>${:.2}</code> | Margin ถืออยู่: <code>${:.2}</code>",
        state.initial_balance,
        state.free_balance,
        state.locked_margin()
    ));
    lines.push(format!(
        "• uPnL: <code>{:+.2} USDT</code> | Realized: <code>{:+.2} USDT</code> | ค่าธรรมเนียม: <code>${:.2}</code>",
        state.unrealized_pnl(),
        state.realized_pnl,
        state.total_fees
    ));
    lines.push(format!(
        "• สถิติ: <code>{} ไม้</code> | Win Rate <code>{:.1}%</code> (<code>{}W/{}L</code>) | Max DD <code>{:.2}%</code>",
        state.total_closed,
        state.win_rate_pct(),
        state.win_count,
        state.loss_count,
        state.max_drawdown_pct
    ));
    lines.push(String::new());

    if state.open_positions.is_empty() {
        lines.push(format!(
            "📌 <b>ออเดอร์ที่ถืออยู่ (0/{}):</b> ยังไม่มี (รอสัญญาณ ≥ {} ดาว)",
            eff_max, eff_stars
        ));
    } else {
        lines.push(format!(
            "📌 <b>ออเดอร์ที่ถืออยู่ ({}/{} ไม้):</b>",
            state.open_positions.len(),
            eff_max
        ));
        for (idx, p) in state.open_positions.iter().enumerate() {
            let side_icon = if p.side == PaperSide::Long {
                "🟢 LONG"
            } else {
                "🔴 SHORT"
            };
            let remain_pct = (p.remaining_fraction * 100.0).round();
            let pos_strat = TradingStrategy::from_str_lossy(&p.strategy).unwrap_or_default();
            let stage = if p.tp2_hit {
                format!("TP1+TP2 (ถือ {:.0}%)", remain_pct)
            } else if p.tp1_hit {
                format!("กันทุน BE (ถือ {:.0}%)", remain_pct)
            } else {
                "รอ TP1 (ถือ 100%)".to_string()
            };
            lines.push(format!(
                "{}) #{} {} <b>{}</b> [{}] | <code>${}</code> → <code>${}</code>",
                idx + 1,
                p.id,
                side_icon,
                html_escape(&p.symbol),
                html_escape(pos_strat.short_tag()),
                format_price(p.entry_price),
                format_price(p.last_price)
            ));
            lines.push(format!(
                "   uPnL: <code>{:+.2} USDT ({:+.1}%)</code> | SL: <code>${}</code> | {}",
                p.unrealized_pnl(),
                p.unrealized_roe_pct(),
                format_price(p.current_sl),
                stage
            ));
        }
    }

    lines.push(format!(
        "\n🕒 <i>อัปเดตล่าสุด: {}</i>",
        Local::now().format("%H:%M:%S")
    ));

    let toggle_btn = if state.paused_opening {
        json!({ "text": "▶️ เปิดรับไม้ใหม่", "callback_data": "paper:resume" })
    } else {
        json!({ "text": "⏸️ พักเปิดไม้ใหม่", "callback_data": "paper:pause" })
    };

    let mut rows = vec![
        vec![
            json!({ "text": "🔄 รีเฟรชพอร์ต", "callback_data": "paper:refresh" }),
            json!({ "text": "📜 ประวัติ 10 รายการ", "callback_data": "paper:history" }),
        ],
        vec![
            toggle_btn,
            json!({ "text": "🔍 สแกนทันที", "callback_data": "paper:scan" }),
        ],
    ];

    if !state.open_positions.is_empty() {
        rows.push(vec![
            json!({ "text": "📋 ปิดออเดอร์รายตัว", "callback_data": "paper:manage" }),
            json!({ "text": "❌ ปิดทุกไม้ทันที", "callback_data": "paper:close_all_prompt" }),
        ]);
    }

    rows.push(vec![
        json!({
            "text": format!("🎯 กลยุทธ์ ({})", eff_strat.short_tag()),
            "callback_data": "paper:strategy"
        }),
        json!({ "text": "⚙️ ตั้งค่าพอร์ต", "callback_data": "paper:settings" }),
    ]);

    rows.push(vec![json!({
        "text": format!("🔄 รีเซ็ตพอร์ต (${:.0})", cfg.paper_initial_balance),
        "callback_data": "paper:reset_prompt"
    })]);

    (lines.join("\n"), json!(rows))
}

pub fn render_strategy_html(
    state: &PaperPortfolioState,
    cfg: &ScannerConfig,
    notice_line: Option<&str>,
) -> (String, Value) {
    let eff_strat = state.effective_strategy(cfg);
    let eff_stars = state.effective_min_stars(cfg);
    let profile = eff_strat.trade_profile();
    let default_stars = eff_strat.default_min_stars(cfg.paper_min_stars);
    let strat_source = if state.custom_strategy.is_some() {
        "🎛️ เลือกผ่าน Telegram"
    } else {
        "📄 ค่าเริ่มต้น .env"
    };

    let mut lines = Vec::new();
    if let Some(note) = notice_line {
        lines.push(format!("ℹ️ <i>{}</i>", html_escape(note)));
        lines.push(String::new());
    }
    lines.push("🎯 <b>เลือกกลยุทธ์การสแกน &amp; จำลองการเทรด</b>".to_string());
    lines.push(format!(
        "• กลยุทธ์ปัจจุบัน: <b>{}</b> (<code>{}</code>)",
        html_escape(eff_strat.title_th()),
        strat_source
    ));
    lines.push(format!(
        "• แนวทาง: <i>{}</i>",
        html_escape(eff_strat.description_th())
    ));
    lines.push(format!(
        "• สัญญาณที่รับ: <code>{}</code>",
        html_escape(eff_strat.signals_summary_th())
    ));
    lines.push(format!(
        "• เงื่อนไข MTF &amp; ดาว: <code>{} | เกณฑ์เข้า ≥ {}⭐ (ค่าเริ่มต้นสายนี้ ≥ {}⭐)</code>",
        if eff_strat.requires_1h_alignment() {
            "บังคับตามเทรนด์ 1h"
        } else {
            "ยืดหยุ่น 1h"
        },
        eff_stars,
        default_stars
    ));
    lines.push(format!(
        "• แผน SL/TP: <code>SL {:.2}%–{:.2}% | TP {:.1}R/{:.1}R/{:.1}R | แบ่งปิด {:.0}/{:.0}/{:.0}%</code>",
        profile.min_risk_pct,
        profile.max_risk_pct,
        profile.tp1_r,
        profile.tp2_r,
        profile.tp3_r,
        profile.tp1_close_pct,
        profile.tp2_close_pct,
        profile.tp3_close_pct
    ));

    lines.push(String::new());
    lines.push("📚 <b>สรุป 5 กลยุทธ์ (กดปุ่มเพื่อสลับได้ทันที):</b>".to_string());
    for s in TradingStrategy::all() {
        let p = s.trade_profile();
        let active_mark = if *s == eff_strat { "✅ " } else { "• " };
        lines.push(format!(
            "{}<b>{}</b> (≥{}⭐ | {}/{}/{}% | Max SL {:.1}%)",
            active_mark,
            html_escape(s.title_th()),
            s.default_min_stars(cfg.paper_min_stars),
            fmt_num_clean(p.tp1_close_pct),
            fmt_num_clean(p.tp2_close_pct),
            fmt_num_clean(p.tp3_close_pct),
            p.max_risk_pct
        ));
    }

    lines.push(String::new());
    lines.push("📊 <b>สถิติ Win Rate / PnL แยกตามกลยุทธ์:</b>".to_string());
    let mut any_stats = false;
    for s in TradingStrategy::all() {
        if let Some(st_perf) = state.strategy_stats.get(s.key()) {
            if st_perf.trades > 0 {
                any_stats = true;
                lines.push(format!(
                    "• {}: <code>{} ไม้</code> | WR <code>{:.1}%</code> (<code>{}W/{}L</code>) | PnL <code>{:+.2} USDT</code>",
                    html_escape(s.short_tag()),
                    st_perf.trades,
                    st_perf.win_rate_pct(),
                    st_perf.wins,
                    st_perf.losses,
                    st_perf.realized_pnl
                ));
            }
        }
    }
    if !any_stats {
        lines.push("• <i>ยังไม่มีประวัติปิดไม้แยกตามกลยุทธ์ (จะบันทึกอัตโนมัติเมื่อปิดออเดอร์)</i>".to_string());
    }

    lines.push(String::new());
    lines.push("💡 <i>สลับกลยุทธ์จะตั้งค่า Min ดาว ตามสายนั้นให้อัตโนมัติ (คง Lev/Size/Max เดิม) และไม้ที่ถืออยู่แล้วยังปิดตามแผนเดิม</i>".to_string());
    lines.push("⌨️ <b>คำสั่งด่วน:</b> <code>/strategy trend</code> · <code>/set strategy scalp lev 15</code>".to_string());

    let strat_btn = |s: TradingStrategy| {
        let label = if s == eff_strat {
            format!("✅ {}", s.title_th())
        } else {
            s.title_th().to_string()
        };
        json!({ "text": label, "callback_data": format!("paper:strat:{}", s.key()) })
    };

    let kb = json!([
        [strat_btn(TradingStrategy::Balanced), strat_btn(TradingStrategy::TrendFollow)],
        [strat_btn(TradingStrategy::Breakout), strat_btn(TradingStrategy::ReversalSniper)],
        [strat_btn(TradingStrategy::ScalpQuick)],
        [
            { "text": "⚙️ ตั้งค่าพอร์ต", "callback_data": "paper:settings" },
            { "text": "⬅️ กลับหน้าพอร์ต", "callback_data": "paper:refresh" }
        ]
    ]);

    (lines.join("\n"), kb)
}

pub fn render_settings_html(
    state: &PaperPortfolioState,
    cfg: &ScannerConfig,
    notice_line: Option<&str>,
) -> (String, Value) {
    let eff_strat = state.effective_strategy(cfg);
    let profile = eff_strat.trade_profile();
    let eff_max = state.effective_max_open_positions(cfg);
    let eff_pct = state.effective_position_pct(cfg);
    let eff_lev = state.effective_leverage(cfg);
    let eff_stars = state.effective_min_stars(cfg);

    let eq = state.total_equity().max(0.0);
    let est_margin = eq * (eff_pct / 100.0);
    let est_notional = est_margin * eff_lev;
    let total_alloc_pct = (eff_max as f64) * eff_pct;

    let source_badge = if state.has_custom_settings() {
        "🎛️ <code>ปรับแต่งผ่าน Telegram</code> (จดจำถาวรข้ามรีสตาร์ท/รีเซ็ตพอร์ต)"
    } else {
        "📄 <code>ใช้ค่าเริ่มต้นจาก .env</code>"
    };
    let stars_desc = if eff_stars >= 3 {
        "เฉพาะ 3⭐ A+ Setup".to_string()
    } else {
        format!("≥ {}⭐ ขึ้นไป ({}–3 ดาว)", eff_stars, eff_stars)
    };

    let mut lines = Vec::new();
    if let Some(note) = notice_line {
        lines.push(format!("ℹ️ <i>{}</i>", html_escape(note)));
        lines.push(String::new());
    }
    lines.push("⚙️ <b>ตั้งค่าจำนวนไม้ & ขนาดไม้พอร์ตจำลอง</b>".to_string());
    lines.push(format!("• แหล่งค่าตั้ง: {}", source_badge));
    lines.push(format!(
        "• กลยุทธ์ที่ใช้งาน: <code>{}</code> (SL {:.2}%–{:.2}% | TP ปิด {:.0}/{:.0}/{:.0}%)",
        html_escape(eff_strat.title_th()),
        profile.min_risk_pct,
        profile.max_risk_pct,
        profile.tp1_close_pct,
        profile.tp2_close_pct,
        profile.tp3_close_pct
    ));
    lines.push(format!(
        "• จำนวนไม้สูงสุด (Max): <code>{} ไม้</code> (กำลังถืออยู่ {}/{})",
        eff_max,
        state.open_positions.len(),
        eff_max
    ));
    lines.push(format!(
        "• ขนาดต่อไม้ (Size): <code>{}%</code> (~<code>${:.2} USDT</code>/ไม้ @ Equity <code>${:.2}</code>)",
        fmt_num_clean(eff_pct),
        est_margin,
        eq
    ));
    lines.push(format!(
        "• ตัวคูณ Leverage: <code>{}x</code> (มูลค่าสัญญา ~<code>${:.2} USDT</code>/ไม้)",
        fmt_num_clean(eff_lev),
        est_notional
    ));
    lines.push(format!("• เกณฑ์เข้าสัญญาณ: <code>{}</code>", stars_desc));
    lines.push(format!(
        "• โควตารวมสูงสุด (Max × Size): <code>{}%</code> | ค่าเริ่มต้น .env: <code>{} | {} ไม้ / {}% / {}x / ≥{}⭐</code>",
        fmt_num_clean(total_alloc_pct),
        cfg.scanner_strategy.short_tag(),
        cfg.paper_max_open_positions,
        fmt_num_clean(cfg.paper_position_pct),
        fmt_num_clean(cfg.paper_leverage),
        cfg.paper_min_stars
    ));

    if eff_lev >= 20.0 {
        lines.push(String::new());
        lines.push(format!(
            "⚠️ <b>คำเตือน Leverage สูง ({}x):</b> กลยุทธ์ {} วาง Stop Loss กว้างสุด ~{:.1}% จึงเสี่ยงถูก <b>Liquidate ล้าง Margin ไม้นั้น (-100%)</b> ก่อนถึงจุด SL หากราคาสะบัดแรง",
            fmt_num_clean(eff_lev),
            html_escape(eff_strat.short_tag()),
            profile.max_risk_pct
        ));
    }
    if total_alloc_pct > 100.0 {
        lines.push(String::new());
        lines.push(format!(
            "⚠️ <b>คำเตือนสัดส่วน Margin เกิน 100% ({} × {}% = {}%):</b> เงินว่างจะถูกใช้เต็มก่อนครบทุกโควตา ไม้ถัดไปจะรอจนกว่ามีเงินว่างคืนหลังปิดไม้/ชน TP1",
            eff_max,
            fmt_num_clean(eff_pct),
            fmt_num_clean(total_alloc_pct)
        ));
    }

    lines.push(String::new());
    lines.push("🎛️ <b>กดปุ่มด้านล่าง (แถว: Max ไม้ → ขนาด% → Leverage → ดาว):</b>".to_string());
    lines.push(
        "💡 <b>หรือพิมพ์ปรับค่าละเอียด:</b> <code>/set strategy trend lev 15</code> · <code>/set max 8 size 12</code> · <code>/set stars 3</code> · <code>/set default</code>"
            .to_string(),
    );
    lines.push("<i>หมายเหตุ: ไม้ที่ถืออยู่แล้วคง Margin/Lev และแผน TP เดิม — ค่าใหม่มีผลตั้งแต่ไม้ถัดไป</i>".to_string());

    let max_row: Vec<Value> = [3usize, 5, 8, 10, 15]
        .iter()
        .map(|&m| {
            let label = if eff_max == m {
                format!("✅{}ไม้", m)
            } else {
                format!("{}ไม้", m)
            };
            json!({ "text": label, "callback_data": format!("paper:set:max:{}", m) })
        })
        .collect();

    let size_row: Vec<Value> = [5.0f64, 10.0, 15.0, 20.0, 25.0]
        .iter()
        .map(|&s| {
            let label = if (eff_pct - s).abs() < 0.01 {
                format!("✅{:.0}%", s)
            } else {
                format!("{:.0}%", s)
            };
            json!({ "text": label, "callback_data": format!("paper:set:size:{:.0}", s) })
        })
        .collect();

    let lev_row: Vec<Value> = [3.0f64, 5.0, 10.0, 15.0, 20.0]
        .iter()
        .map(|&l| {
            let label = if (eff_lev - l).abs() < 0.01 {
                format!("✅{:.0}x", l)
            } else {
                format!("{:.0}x", l)
            };
            json!({ "text": label, "callback_data": format!("paper:set:lev:{:.0}", l) })
        })
        .collect();

    let stars_row = vec![
        json!({
            "text": if eff_stars == 2 { "✅ 2⭐ ขึ้นไป" } else { "2⭐ ขึ้นไป" },
            "callback_data": "paper:set:stars:2"
        }),
        json!({
            "text": if eff_stars == 3 { "✅ เฉพาะ 3⭐ A+" } else { "เฉพาะ 3⭐ A+" },
            "callback_data": "paper:set:stars:3"
        }),
    ];

    let strat_footer_row = vec![
        json!({
            "text": format!("🎯 กลยุทธ์ ({})", eff_strat.short_tag()),
            "callback_data": "paper:strategy"
        }),
        json!({ "text": "🔄 คืนค่าเริ่มต้น (.env)", "callback_data": "paper:cfg_default" }),
    ];
    let back_row = vec![json!({ "text": "⬅️ กลับหน้าพอร์ต", "callback_data": "paper:refresh" })];

    let kb = json!([max_row, size_row, lev_row, stars_row, strat_footer_row, back_row]);
    (lines.join("\n"), kb)
}

pub fn render_manage_html(state: &PaperPortfolioState) -> (String, Value) {
    if state.open_positions.is_empty() {
        return (
            "📋 <b>จัดการออเดอร์ที่เปิดอยู่</b>\n\nไม่มีออเดอร์ที่ถืออยู่ในขณะนี้".to_string(),
            json!([[{ "text": "⬅️ กลับหน้าพอร์ต", "callback_data": "paper:refresh" }]]),
        );
    }

    let mut lines = vec![
        "📋 <b>จัดการออเดอร์ที่เปิดอยู่ (เลือกกดเพื่อปิดทันที)</b>".to_string(),
        String::new(),
    ];
    let mut rows = Vec::new();
    for p in &state.open_positions {
        let upnl = p.unrealized_pnl();
        let uroe = p.unrealized_roe_pct();
        let side_icon = if p.side == PaperSide::Long {
            "🟢"
        } else {
            "🔴"
        };
        let pos_strat = TradingStrategy::from_str_lossy(&p.strategy).unwrap_or_default();
        lines.push(format!(
            "• #{} {} <b>{}</b> [{}] | ราคา <code>${}</code> | uPnL <code>{:+.2} ({:+.1}%)</code>",
            p.id,
            side_icon,
            html_escape(&p.symbol),
            html_escape(pos_strat.short_tag()),
            format_price(p.last_price),
            upnl,
            uroe
        ));
        rows.push(vec![json!({
            "text": format!("❌ ปิด #{} {} ({:+.2} USDT)", p.id, p.symbol, upnl),
            "callback_data": format!("paper:close:{}", p.id)
        })]);
    }
    rows.push(vec![
        json!({ "text": "⬅️ กลับหน้าพอร์ต", "callback_data": "paper:refresh" }),
        json!({ "text": "❌ ปิดทุกไม้", "callback_data": "paper:close_all_prompt" }),
    ]);

    (lines.join("\n"), json!(rows))
}

pub fn render_history_html(rows: &[CsvTradeRow]) -> (String, Value) {
    let mut lines = vec!["📜 <b>ประวัติจำลองการเทรด 10 รายการล่าสุด</b>".to_string(), String::new()];
    if rows.is_empty() {
        lines.push("ยังไม่มีประวัติการเปิด/ปิดออเดอร์จำลอง".to_string());
    } else {
        for r in rows {
            let short_ts = if r.timestamp.len() >= 19 {
                &r.timestamp[11..19]
            } else {
                &r.timestamp
            };
            let icon = match r.event.as_str() {
                "OPEN" => "🧪",
                "TP1_PARTIAL" => "💰",
                "TP2_PARTIAL" => "🚀",
                "TP3_FULL_CLOSE" => "🏆",
                "BREAKEVEN_SL" | "TRAILING_SL_TP1" => "✅",
                "MANUAL_CLOSE" => "❌",
                "STOP_LOSS" => "🛑",
                _ => "📌",
            };
            let strat_badge = if r.strategy.is_empty() {
                String::new()
            } else {
                let short = TradingStrategy::from_str_lossy(&r.strategy)
                    .map(|s| s.short_tag().to_string())
                    .unwrap_or_else(|| r.strategy.clone());
                format!(" [{}]", html_escape(&short))
            };
            lines.push(format!(
                "{} <code>{}</code> #{} <b>{}</b> ({}){} | <code>{}</code> | ${} | <code>{:+.2} USDT</code>",
                icon,
                html_escape(short_ts),
                html_escape(&r.trade_id),
                html_escape(&r.symbol),
                html_escape(&r.side),
                strat_badge,
                html_escape(&r.event),
                format_price(r.exec_price),
                r.net_pnl
            ));
        }
    }
    let kb = json!([[
        { "text": "🔄 รีเฟรชประวัติ", "callback_data": "paper:history" },
        { "text": "⬅️ กลับหน้าพอร์ต", "callback_data": "paper:refresh" }
    ]]);
    (lines.join("\n"), kb)
}

pub fn render_close_all_confirm_html(state: &PaperPortfolioState) -> (String, Value) {
    if state.open_positions.is_empty() {
        return (
            "ℹ️ ไม่มีออเดอร์ที่เปิดอยู่ในขณะนี้".to_string(),
            json!([[{ "text": "⬅️ กลับหน้าพอร์ต", "callback_data": "paper:refresh" }]]),
        );
    }
    let text = format!(
        "⚠️ <b>ยืนยันปิดออเดอร์จำลองทั้งหมด ({} ไม้)</b>\n\nระบบจะปิดทุกไม้ที่ถืออยู่ด้วยราคาตลาดปัจจุบันทันที (uPnL รวม <code>{:+.2} USDT</code>)\nต้องการดำเนินการต่อหรือไม่?",
        state.open_positions.len(),
        state.unrealized_pnl()
    );
    let kb = json!([[
        { "text": "✅ ยืนยันปิดทุกไม้ทันที", "callback_data": "paper:close_all_confirm" },
        { "text": "⬅️ ยกเลิก", "callback_data": "paper:refresh" }
    ]]);
    (text, kb)
}

pub fn render_reset_confirm_html(cfg: &ScannerConfig) -> (String, Value) {
    let text = format!(
        "⚠️ <b>ยืนยันการรีเซ็ตพอร์ตจำลอง</b>\n\nการรีเซ็ตจะล้างออเดอร์ที่ถืออยู่ทั้งหมด สถิติ Win Rate และไฟล์ประวัติเทรด CSV แล้วตั้งต้นใหม่เป็น <code>${:.2} USDT</code> <i>(คงกลยุทธ์และค่าตั้ง Max ไม้/Size/Lev ที่ปรับไว้ — ล้างเฉพาะสถิติแยกกลยุทธ์)</i>\nต้องการยืนยันหรือไม่?",
        cfg.paper_initial_balance
    );
    let kb = json!([[
        { "text": "✅ ยืนยันรีเซ็ตพอร์ต", "callback_data": "paper:reset_confirm" },
        { "text": "⬅️ ยกเลิก", "callback_data": "paper:refresh" }
    ]]);
    (text, kb)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paper_trader::PaperPosition;
    use crate::strategy::StrategyPerfStats;

    #[test]
    fn test_render_dashboard_and_buttons() {
        let mut cfg = ScannerConfig::from_env();
        cfg.paper_initial_balance = 1000.0;
        cfg.paper_leverage = 10.0;
        cfg.paper_position_pct = 10.0;
        cfg.paper_max_open_positions = 5;
        cfg.paper_min_stars = 2;

        let mut st = PaperPortfolioState::new(1000.0);
        st.custom_strategy = Some("TREND_FOLLOW".to_string());
        st.open_positions.push(PaperPosition {
            id: 1,
            symbol: "BTC&ETH<TEST>".to_string(),
            side: PaperSide::Long,
            stars: 3,
            signal_category: "RSI_OS".to_string(),
            timeframe: "5m+15m+1h".to_string(),
            strategy: "TREND_FOLLOW".to_string(),
            tp1_close_pct: 25.0,
            tp2_close_pct: 35.0,
            entry_price: 95000.0,
            initial_sl: 94000.0,
            current_sl: 95100.0,
            tp1: 96000.0,
            tp2: 97000.0,
            tp3: 98500.0,
            leverage: 10.0,
            initial_margin: 100.0,
            remaining_margin: 75.0,
            initial_qty: 0.01,
            remaining_qty: 0.0075,
            remaining_fraction: 0.75,
            tp1_hit: true,
            tp2_hit: false,
            realized_pnl_trade: 4.0,
            fees_paid_trade: 0.5,
            last_price: 95800.0,
            opened_at: "2026-11-16T00:00:00Z".to_string(),
        });

        let (html, kb) = render_dashboard_html(&st, &cfg, Some("ทดสอบ <แจ้งเตือน>"));
        assert!(html.contains("&amp;"));
        assert!(html.contains("&lt;TEST&gt;"));
        assert!(html.contains("[Trend]"));
        assert!(html.contains("กันทุน BE (ถือ 75%)"));
        assert!(html.contains("10x | ไม้ละ 10% | สูงสุด 5 ไม้"));
        let rows = kb.as_array().unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[3][0]["callback_data"], "paper:strategy");
        assert_eq!(rows[3][1]["callback_data"], "paper:settings");
        assert_eq!(rows[4][0]["callback_data"], "paper:reset_prompt");
    }

    #[test]
    fn test_render_settings_warnings_and_presets() {
        let mut cfg = ScannerConfig::from_env();
        cfg.paper_initial_balance = 1000.0;
        cfg.paper_leverage = 10.0;
        cfg.paper_position_pct = 10.0;
        cfg.paper_max_open_positions = 5;
        cfg.paper_min_stars = 2;

        let mut st = PaperPortfolioState::new(1000.0);
        st.custom_strategy = Some("SCALP_QUICK".to_string());
        st.custom_max_open_positions = Some(8);
        st.custom_position_pct = Some(20.0);
        st.custom_leverage = Some(20.0);
        st.custom_min_stars = Some(3);

        let (html, kb) = render_settings_html(&st, &cfg, Some("อัปเดตสำเร็จ"));
        assert!(html.contains("อัปเดตสำเร็จ"));
        assert!(html.contains("ปรับแต่งผ่าน Telegram"));
        assert!(html.contains("คำเตือน Leverage สูง (20x)"));
        assert!(html.contains("~2.8%"));
        assert!(html.contains("คำเตือนสัดส่วน Margin เกิน 100% (8 × 20% = 160%)"));
        let rows = kb.as_array().unwrap();
        assert_eq!(rows.len(), 6);
        assert_eq!(rows[0][2]["text"], "✅8ไม้");
        assert_eq!(rows[1][3]["text"], "✅20%");
        assert_eq!(rows[2][4]["text"], "✅20x");
        assert_eq!(rows[3][1]["text"], "✅ เฉพาะ 3⭐ A+");
        assert_eq!(rows[4][0]["callback_data"], "paper:strategy");
        assert_eq!(rows[4][1]["callback_data"], "paper:cfg_default");
        assert_eq!(rows[5][0]["callback_data"], "paper:refresh");
    }

    #[test]
    fn test_render_strategy_html_and_stats() {
        let mut cfg = ScannerConfig::from_env();
        cfg.paper_min_stars = 2;
        let mut st = PaperPortfolioState::new(1000.0);
        st.custom_strategy = Some("TREND_FOLLOW".to_string());
        st.strategy_stats.insert(
            "TREND_FOLLOW".to_string(),
            StrategyPerfStats {
                trades: 4,
                wins: 3,
                losses: 1,
                realized_pnl: 42.5,
            },
        );

        let (html, kb) = render_strategy_html(&st, &cfg, Some("สลับสำเร็จ <ok>"));
        assert!(html.contains("สลับสำเร็จ &lt;ok&gt;"));
        assert!(html.contains("Trend Follow"));
        assert!(html.contains("บังคับตามเทรนด์ 1h"));
        assert!(html.contains("75.0%"));
        assert!(html.contains("+42.50 USDT"));
        let rows = kb.as_array().unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0][1]["callback_data"], "paper:strat:TREND_FOLLOW");
        assert!(rows[0][1]["text"].as_str().unwrap().starts_with("✅ "));
    }

    #[test]
    fn test_parse_set_command_args() {
        assert_eq!(
            parse_set_command_args("default").unwrap(),
            ParsedSetCommand::ResetToDefault
        );
        assert_eq!(
            parse_set_command_args("max 8 size 12.5% lev 15x stars 3").unwrap(),
            ParsedSetCommand::Update {
                max_open_positions: Some(8),
                position_pct: Some(12.5),
                leverage: Some(15.0),
                min_stars: Some(3),
                strategy: None,
            }
        );
        assert_eq!(
            parse_set_command_args("strat trend lev 15").unwrap(),
            ParsedSetCommand::Update {
                max_open_positions: None,
                position_pct: None,
                leverage: Some(15.0),
                min_stars: None,
                strategy: Some(TradingStrategy::TrendFollow),
            }
        );
        assert_eq!(
            parse_set_command_args("max=50 lev=200").unwrap(),
            ParsedSetCommand::Update {
                max_open_positions: Some(30),
                position_pct: None,
                leverage: Some(125.0),
                min_stars: None,
                strategy: None,
            }
        );
        assert!(parse_set_command_args("lev").is_err());
        assert!(parse_set_command_args("unknown 10").is_err());
        assert!(parse_set_command_args("strategy nope").is_err());
        assert!(parse_set_command_args("max 0").is_err());
    }
}
