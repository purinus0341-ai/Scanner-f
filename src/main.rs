pub mod binance;
pub mod config;
pub mod indicators;
pub mod models;
pub mod notifier;
pub mod paper_trader;
pub mod scanner;
pub mod telegram_bot;

use std::env;
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use tokio::sync::Notify;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::binance::BinanceFuturesClient;
use crate::config::ScannerConfig;
use crate::models::{ScanSignal, SignalCategory, SignalDirection};
use crate::notifier::{format_compact_usd, AlertNotifier};
use crate::paper_trader::PaperTrader;
use crate::scanner::FuturesScanner;
use crate::telegram_bot::TelegramBotController;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    let args: Vec<String> = env::args().collect();
    let run_once = args.iter().any(|a| a == "--once");
    let test_alert = args.iter().any(|a| a == "--test-alert");
    let paper_status = args.iter().any(|a| a == "--paper-status");
    let paper_reset = args.iter().any(|a| a == "--paper-reset");
    let send_telegram_flag = args.iter().any(|a| a == "--send");

    let config = ScannerConfig::from_env();
    print_startup_banner(&config);

    let notifier = AlertNotifier::new(config.clone());
    let paper_trader = PaperTrader::new(config.clone(), notifier.clone());
    let client = BinanceFuturesClient::new(config.clone());
    let bot_ctrl = TelegramBotController::new(
        config.clone(),
        client,
        notifier.clone(),
        paper_trader.clone(),
        Arc::new(Notify::new()),
    );

    if paper_reset {
        paper_trader.reset_portfolio()?;
        info!(
            "🔄 รีเซ็ตพอร์ตจำลองการเทรดเป็นทุนเริ่มต้น ${:.2} USDT เรียบร้อยแล้ว",
            config.paper_initial_balance
        );
        return Ok(());
    }

    if paper_status {
        paper_trader.print_portfolio_summary().await;
        if send_telegram_flag {
            bot_ctrl
                .send_dashboard_message(true, Some("💼 รายงานพอร์ตจำลองตามคำสั่ง"))
                .await;
        }
        return Ok(());
    }

    if test_alert {
        info!("กำลังส่งสัญญาณทดสอบ (Test Alert) ไปยังช่องทางที่ตั้งค่าไว้...");
        let sample_signal = ScanSignal {
            symbol: "BTCUSDT".to_string(),
            category: SignalCategory::PricePump,
            direction: SignalDirection::Bullish,
            price: 95420.50,
            change_24h_pct: Some(4.85),
            quote_volume_24h: Some(18_500_000_000.0),
            timeframe: config.kline_interval.clone(),
            metric_summary: "ทดสอบระบบแจ้งเตือน Scanner-f (Price Pump +3.25% & Volume Spike 4.2x)"
                .to_string(),
            details: vec![
                ("RSI(14)".to_string(), "76.40".to_string()),
                ("EMA(9/21)".to_string(), "Bullish Golden Cross".to_string()),
                ("Funding Rate".to_string(), "+0.0100%".to_string()),
            ],
            timestamp: Utc::now(),
        };
        notifier.send_test_notification(&sample_signal).await?;
        bot_ctrl
            .send_dashboard_message(false, Some("🧪 ทดสอบแสดงผลพอร์ตจำลอง + ปุ่มสั่งการบน Telegram"))
            .await;
        return Ok(());
    }

    let scanner = Arc::new(FuturesScanner::new(config.clone(), notifier, paper_trader));

    if run_once {
        let (symbols, signal_count) = scanner.initialize_and_scan_once(true).await?;
        info!(
            "สแกนแบบครั้งเดียว (--once) เสร็จสมบูรณ์: ตรวจสอบ {} เหรียญ, พบสัญญาณทั้งหมด {} รายการ",
            symbols.len(),
            signal_count
        );
        bot_ctrl
            .send_dashboard_message(false, Some("📊 สรุปพอร์ตจำลองหลังสแกนรอบล่าสุด"))
            .await;
        return Ok(());
    }

    tokio::select! {
        res = scanner.run_forever() => {
            if let Err(err) = res {
                tracing::error!("Scanner หยุดทำงานด้วยข้อผิดพลาด: {:#}", err);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("ได้รับคำสั่งหยุดการทำงาน (Ctrl+C) ปิดระบบเรียบร้อย");
        }
    }

    Ok(())
}

fn print_startup_banner(cfg: &ScannerConfig) {
    info!("==============================================================");
    info!("⚡ Scanner-f : ระบบสแกนเนอร์ Binance USDT-M Futures (Rust) ⚡");
    info!("==============================================================");
    info!(
        "• เกณฑ์คัดกรองเหรียญ : USDT-M Perpetual | 24h Volume >= ${} (สูงสุด {} เหรียญ)",
        format_compact_usd(cfg.min_quote_volume_24h),
        cfg.max_symbols
    );
    info!(
        "• Timeframe หลัก   : {} (ย้อนหลัง {} แท่ง)",
        cfg.kline_interval, cfg.candle_buffer_size
    );
    info!(
        "• สัญญาณราคา/วอลุ่ม : Pump/Dump >= ±{:.2}% | Volume Spike >= {:.1}x ({} แท่ง)",
        cfg.price_change_threshold_pct, cfg.volume_spike_multiplier, cfg.volume_lookback
    );
    info!(
        "• อินดิเคเตอร์เทคนิค  : RSI({}) [OB >= {:.0}, OS <= {:.0}] | EMA({}/{}) Cross | BB({}, {:.1}σ)",
        cfg.rsi_period,
        cfg.rsi_overbought,
        cfg.rsi_oversold,
        cfg.ema_fast_period,
        cfg.ema_slow_period,
        cfg.bb_period,
        cfg.bb_std_dev
    );
    info!(
        "• ข้อมูล Futures    : Funding Rate >= ±{:.3}% | OI Spike >= ±{:.2}% | Liquidation >= ${}",
        cfg.funding_rate_threshold_pct,
        cfg.oi_spike_threshold_pct,
        format_compact_usd(cfg.min_liquidation_usd)
    );
    info!(
        "• จำลองการเทรด     : {} | ทุน ${:.0} | {:.0}x | ไม้ละ {:.0}% | Max {} ไม้ | >= {} ดาว",
        if cfg.paper_trading_enabled { "เปิด ✅" } else { "ปิด ⚪" },
        cfg.paper_initial_balance,
        cfg.paper_leverage,
        cfg.paper_position_pct,
        cfg.paper_max_open_positions,
        cfg.paper_min_stars
    );
    info!(
        "• ช่องทางแจ้งเตือน   : Telegram={} | Discord Webhook={} | Cooldown={}s",
        if cfg.has_telegram() { "เปิดใช้งาน ✅" } else { "ยังไม่ตั้งค่า ⚪" },
        if cfg.has_discord() { "เปิดใช้งาน ✅" } else { "ยังไม่ตั้งค่า ⚪" },
        cfg.alert_cooldown_secs
    );
    info!("==============================================================");
}
