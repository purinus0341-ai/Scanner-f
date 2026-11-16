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
                let cmd = text
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .split('@')
                    .next()
                    .unwrap_or("")
                    .to_lowercase();
                self.handle_command(&cmd).await;
            }
        }
    }

    async fn handle_command(&self, cmd: &str) {
        match cmd {
            "/start" | "/portfolio" | "/status" | "/positions" => {
                self.send_dashboard_message(true, None).await;
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
                    "• /history — ดูประวัติการเทรด 10 รายการล่าสุด\n",
                    "• /scan — สแกนตลาดและอัปเดตราคาทันที\n",
                    "• /pause — พักการเปิดออเดอร์ใหม่ชั่วคราว\n",
                    "• /resume — เปิดรับออเดอร์ใหม่อีกครั้ง\n",
                    "• /closeall — ปิดออเดอร์จำลองทั้งหมดทันที (มีหน้ายืนยัน)\n",
                    "• /reset — รีเซ็ตพอร์ตจำลองกลับเป็นทุนเริ่มต้น (มีหน้ายืนยัน)"
                );
                let kb = json!([[{ "text": "💼 เปิดพอร์ตจำลอง + ปุ่มสั่งการ", "callback_data": "paper:panel" }]]);
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
                    Some("▶️ เปิดรับออเดอร์จำลองจากสัญญาณ 2-3 ดาวแล้ว"),
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
                        Some("🔄 รีเซ็ตพอร์ตและล้างประวัติเรียบร้อยแล้ว"),
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

    let mut lines = Vec::new();
    if let Some(note) = notice_line {
        lines.push(format!("ℹ️ <i>{}</i>", html_escape(note)));
        lines.push(String::new());
    }
    lines.push("💼 <b>พอร์ตจำลองการเทรด (Paper Trading Dashboard)</b>".to_string());
    lines.push(format!("• สถานะเปิดไม้ใหม่: <code>{}</code>", status_badge));
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
            cfg.paper_max_open_positions, cfg.paper_min_stars
        ));
    } else {
        lines.push(format!(
            "📌 <b>ออเดอร์ที่ถืออยู่ ({}/{} ไม้):</b>",
            state.open_positions.len(),
            cfg.paper_max_open_positions
        ));
        for (idx, p) in state.open_positions.iter().enumerate() {
            let side_icon = if p.side == PaperSide::Long {
                "🟢 LONG"
            } else {
                "🔴 SHORT"
            };
            let stage = if p.tp2_hit {
                "TP1+TP2 (ถือ 30%)"
            } else if p.tp1_hit {
                "กันทุน BE (ถือ 60%)"
            } else {
                "รอ TP1 (ถือ 100%)"
            };
            lines.push(format!(
                "{}) #{} {} <b>{}</b> | <code>${}</code> → <code>${}</code>",
                idx + 1,
                p.id,
                side_icon,
                html_escape(&p.symbol),
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

    rows.push(vec![json!({
        "text": format!("🔄 รีเซ็ตพอร์ต (${:.0})", cfg.paper_initial_balance),
        "callback_data": "paper:reset_prompt"
    })]);

    (lines.join("\n"), json!(rows))
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
        lines.push(format!(
            "• #{} {} <b>{}</b> | ราคา <code>${}</code> | uPnL <code>{:+.2} ({:+.1}%)</code>",
            p.id,
            side_icon,
            html_escape(&p.symbol),
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
            lines.push(format!(
                "{} <code>{}</code> #{} <b>{}</b> ({}) | <code>{}</code> | ${} | <code>{:+.2} USDT</code>",
                icon,
                html_escape(short_ts),
                html_escape(&r.trade_id),
                html_escape(&r.symbol),
                html_escape(&r.side),
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
        "⚠️ <b>ยืนยันการรีเซ็ตพอร์ตจำลอง</b>\n\nการรีเซ็ตจะล้างออเดอร์ที่ถืออยู่ทั้งหมด สถิติ Win Rate และไฟล์ประวัติเทรด CSV แล้วตั้งต้นใหม่เป็น <code>${:.2} USDT</code>\nต้องการยืนยันหรือไม่?",
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

    #[test]
    fn test_render_dashboard_and_buttons() {
        let cfg = ScannerConfig::from_env();
        let mut st = PaperPortfolioState::new(1000.0);
        st.open_positions.push(PaperPosition {
            id: 1,
            symbol: "BTC&ETH<TEST>".to_string(),
            side: PaperSide::Long,
            stars: 3,
            signal_category: "RSI_OS".to_string(),
            timeframe: "5m+15m+1h".to_string(),
            entry_price: 95000.0,
            initial_sl: 94000.0,
            current_sl: 95100.0,
            tp1: 96000.0,
            tp2: 97000.0,
            tp3: 98500.0,
            leverage: 10.0,
            initial_margin: 100.0,
            remaining_margin: 60.0,
            initial_qty: 0.01,
            remaining_qty: 0.006,
            remaining_fraction: 0.6,
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
        assert!(html.contains("กันทุน BE (ถือ 60%)"));
        let rows = kb.as_array().unwrap();
        assert_eq!(rows.len(), 4);
    }
}
