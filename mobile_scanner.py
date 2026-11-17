#!/usr/bin/env python3
"""
📱 Scanner-f Mobile Edition (Multi-Timeframe 5m + 15m + 1h)
วิเคราะห์ 3 Timeframe พร้อมกัน:
  • 5m  = จังหวะเข้าคมๆ (Entry Trigger)
  • 15m = ยืนยันโมเมนตัม & วางโครงสร้าง Stop Loss กันไส้เทียนสะบัด
  • 1h  = คุมเทรนด์หลัก (Major Trend) & เป้ารันเทรนด์ (TP3)
คัดกรองเฉพาะสัญญาณระดับ ⭐⭐ (2 ดาว) และ ⭐⭐⭐ (3 ดาว A+ Setup)
"""

import concurrent.futures
import copy
import html
import json
import math
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone


def load_dotenv(path=".env"):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


load_dotenv()

FAPI_URL = os.environ.get("BINANCE_FAPI_URL", "https://fapi.binance.com").rstrip("/")
MIN_QUOTE_VOL_24H = float(os.environ.get("MIN_QUOTE_VOLUME_24H", "20000000"))
MAX_SYMBOLS = int(os.environ.get("MAX_SYMBOLS", "100"))
CANDLE_LIMIT = int(os.environ.get("CANDLE_BUFFER_SIZE", "65"))

PRICE_CHANGE_PCT = float(os.environ.get("PRICE_CHANGE_THRESHOLD_PCT", "2.2"))
VOL_SPIKE_MULT = float(os.environ.get("VOLUME_SPIKE_MULTIPLIER", "3.2"))
VOL_LOOKBACK = int(os.environ.get("VOLUME_LOOKBACK", "20"))

RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))
RSI_OB = float(os.environ.get("RSI_OVERBOUGHT", "72.0"))
RSI_OS = float(os.environ.get("RSI_OVERSOLD", "28.0"))

EMA_FAST = int(os.environ.get("EMA_FAST_PERIOD", "9"))
EMA_SLOW = int(os.environ.get("EMA_SLOW_PERIOD", "21"))
EMA_TREND = 50

BB_PERIOD = int(os.environ.get("BB_PERIOD", "20"))
BB_STD = float(os.environ.get("BB_STD_DEV", "2.0"))
BB_MIN_VOL_RATIO = float(os.environ.get("BB_MIN_VOLUME_RATIO", "1.6"))

FUNDING_THRESHOLD_PCT = float(os.environ.get("FUNDING_RATE_THRESHOLD_PCT", "0.10"))
SCAN_INTERVAL_SECS = int(os.environ.get("FUTURES_POLL_INTERVAL_SECS", "60"))
ALERT_COOLDOWN_SECS = int(os.environ.get("ALERT_COOLDOWN_SECS", "900"))
MIN_STARS = int(os.environ.get("MIN_CONFLUENCE_STARS", "2"))

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()


def env_bool(key, default=True):
    val = os.environ.get(key)
    if val is None:
        return default
    v = val.strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return default


PAPER_TRADING_ENABLED = env_bool("PAPER_TRADING_ENABLED", True)
PAPER_INITIAL_BALANCE = float(os.environ.get("PAPER_INITIAL_BALANCE", "1000.0"))
PAPER_LEVERAGE = float(os.environ.get("PAPER_LEVERAGE", "10.0"))
PAPER_POSITION_PCT = float(os.environ.get("PAPER_POSITION_PCT", "10.0"))
PAPER_MAX_OPEN_POSITIONS = int(os.environ.get("PAPER_MAX_OPEN_POSITIONS", "5"))
PAPER_MIN_STARS = max(1, min(3, int(os.environ.get("PAPER_MIN_STARS", "2"))))
PAPER_TAKER_FEE_PCT = float(os.environ.get("PAPER_TAKER_FEE_PCT", "0.05"))
PAPER_NOTIFY_WEBHOOKS = env_bool("PAPER_NOTIFY_WEBHOOKS", True)
PAPER_TELEGRAM_BOT_ENABLED = env_bool("PAPER_TELEGRAM_BOT_ENABLED", True)
PAPER_TELEGRAM_SUMMARY_MINS = int(os.environ.get("PAPER_TELEGRAM_SUMMARY_MINS", "0"))
PAPER_STATE_FILE = os.environ.get("PAPER_STATE_FILE", "paper_state.json")
PAPER_TRADES_CSV = os.environ.get("PAPER_TRADES_CSV", "paper_trades.csv")

cooldown_cache = {}
# Cache สำหรับแท่งเทียน 15m และ 1h เพื่อประหยัดเน็ตและทำให้สแกนไวขึ้นบนมือถือ
htf_candle_cache = {
    "15m": {},  # symbol -> (timestamp, candles)
    "1h": {},   # symbol -> (timestamp, candles)
}


def http_get_json(url, timeout=12):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "scanner-f-mtf-mobile/2.0"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_post_json(url, payload, timeout=10):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "scanner-f-mtf-mobile/2.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        err_body = ""
        try:
            err_body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            pass
        raise RuntimeError(f"HTTP {e.code}: {err_body or e.reason}") from e


def fmt_price(p):
    if p >= 1000:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.01:
        return f"{p:.5f}"
    return f"{p:.7f}"


def fmt_usd(val):
    a = abs(val)
    if a >= 1_000_000_000:
        return f"{val / 1_000_000_000:.2f}B"
    if a >= 1_000_000:
        return f"{val / 1_000_000:.2f}M"
    if a >= 1_000:
        return f"{val / 1_000:.2f}K"
    return f"{val:.2f}"


def calc_rsi(closes, period=14):
    if len(closes) <= period:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d > 0:
            gains += d
        else:
            losses -= d
    avg_gain = gains / period
    avg_loss = losses / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        gain = d if d > 0 else 0.0
        loss = -d if d < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def calc_ema_series(closes, period):
    if len(closes) < period:
        return []
    sma = sum(closes[:period]) / period
    emas = [sma] * period
    k = 2.0 / (period + 1.0)
    prev = sma
    for price in closes[period:]:
        ema = (price - prev) * k + prev
        emas.append(ema)
        prev = ema
    return emas


def calc_bollinger(closes, period=20, std_mult=2.0):
    if len(closes) < period:
        return None
    win = closes[-period:]
    mean = sum(win) / period
    var = sum((x - mean) ** 2 for x in win) / period
    sd = math.sqrt(var)
    return mean + std_mult * sd, mean, mean - std_mult * sd


def calc_atr(candles, period=14):
    if not candles or len(candles) < 2:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        prev_c = candles[i - 1]["close"]
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        trs.append(tr)
    if not trs:
        return 0.0
    window = trs[-period:] if len(trs) >= period else trs
    return sum(window) / len(window)


def fetch_klines(symbol, interval, limit=CANDLE_LIMIT):
    url = f"{FAPI_URL}/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    rows = http_get_json(url)
    candles = []
    for idx, r in enumerate(rows):
        candles.append(
            {
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),
                "quote_vol": float(r[7]),
                "closed": idx + 1 < len(rows),
            }
        )
    return candles


def get_cached_klines(symbol, interval, ttl_secs):
    now = time.time()
    cache_bucket = htf_candle_cache.get(interval, {})
    if symbol in cache_bucket:
        ts, candles = cache_bucket[symbol]
        if now - ts < ttl_secs:
            return candles
    candles = fetch_klines(symbol, interval, CANDLE_LIMIT)
    cache_bucket[symbol] = (now, candles)
    return candles


def analyze_tf_state(candles):
    """
    วิเคราะห์สถานะของ Timeframe นั้นๆ (EMA 9/21/50, RSI 14, Volume Ratio, ATR 14)
    คืนค่า dict สรุปสถานะว่าเป็น BULL, BEAR หรือ NEUTRAL พร้อมรายละเอียด
    """
    if not candles or len(candles) < 25:
        return None

    latest = candles[-1]
    closed = candles[:-1] if not latest["closed"] else candles
    closes = [c["close"] for c in closed]
    price = latest["close"]

    rsi = calc_rsi(closes, RSI_PERIOD) or 50.0
    ema9_list = calc_ema_series(closes, EMA_FAST)
    ema21_list = calc_ema_series(closes, EMA_SLOW)
    ema50_list = calc_ema_series(closes, EMA_TREND)

    ema9 = ema9_list[-1] if ema9_list else price
    ema21 = ema21_list[-1] if ema21_list else price
    ema50 = ema50_list[-1] if ema50_list else ema21

    prev_vols = [c["quote_vol"] for c in candles[-1 - VOL_LOOKBACK : -1]] if len(candles) > VOL_LOOKBACK else []
    avg_vol = (sum(prev_vols) / len(prev_vols)) if prev_vols else 0.0
    vol_ratio = (latest["quote_vol"] / avg_vol) if avg_vol > 0 else 1.0
    atr = calc_atr(closed, 14)

    # ประเมิน Bias ของ Timeframe:
    # BULLISH เมื่อ EMA9 > EMA21 และราคา >= EMA21 (หรือเหนือ EMA50)
    # BEARISH เมื่อ EMA9 < EMA21 และราคา <= EMA21 (หรือใต้ EMA50)
    if ema9 > ema21 and price >= ema21:
        trend = "BULL"
        label = f"🟢 ขาขึ้น (EMA9>21, RSI {rsi:.1f})"
    elif ema9 < ema21 and price <= ema21:
        trend = "BEAR"
        label = f"🔴 ขาลง (EMA9<21, RSI {rsi:.1f})"
    else:
        if rsi >= 55 and price >= ema50:
            trend = "BULL"
            label = f"🟢 พักตัวขาขึ้น (RSI {rsi:.1f})"
        elif rsi <= 45 and price <= ema50:
            trend = "BEAR"
            label = f"🔴 พักตัวขาลง (RSI {rsi:.1f})"
        else:
            trend = "NEUTRAL"
            label = f"⚪ ไซด์เวย์ (RSI {rsi:.1f})"

    if rsi <= RSI_OS:
        label += " [Oversold 🧊]"
    elif rsi >= RSI_OB:
        label += " [Overbought 🔥]"

    return {
        "trend": trend,
        "rsi": rsi,
        "ema9": ema9,
        "ema21": ema21,
        "ema50": ema50,
        "vol_ratio": vol_ratio,
        "atr": atr,
        "label": label,
        "candles": candles,
        "closed": closed,
    }


def evaluate_mtf_confluence(direction, is_reversal, state_5m, state_15m, state_1h):
    """
    คำนวณคะแนนความสอดคล้อง Multi-Timeframe (1 ถึง 3 ดาว)
    - direction: "BULL" หรือ "BEAR"
    - is_reversal: True หากเป็นสัญญาณกลับตัว (เช่น RSI Oversold/Overbought)
    """
    score = 1  # มีสัญญาณตั้งต้นจาก TF ที่เกิด Trigger แล้ว 1 คะแนน
    reasons = []

    rsi_5m = state_5m["rsi"]
    rsi_15m = state_15m["rsi"]
    rsi_1h = state_1h["rsi"]

    # 1. เช็คความสอดคล้องกับ 15m (ยืนยันโมเมนตัม / จุดกลับตัว)
    if direction == "BULL":
        if state_15m["trend"] == "BULL":
            score += 1
            reasons.append("15m โมเมนตัมขาขึ้นหนุน")
        elif is_reversal and rsi_15m <= (RSI_OS + 5.0):
            score += 1
            reasons.append(f"15m เข้าเขต Oversold ร่วมด้วย (RSI {rsi_15m:.1f})")
        elif state_5m["vol_ratio"] >= 2.2 and rsi_15m >= 48.0:
            score += 1
            reasons.append(f"5m วอลุ่มเข้าหนาแน่น ({state_5m['vol_ratio']:.1f}x)")
    else:
        if state_15m["trend"] == "BEAR":
            score += 1
            reasons.append("15m โมเมนตัมขาลงกดดัน")
        elif is_reversal and rsi_15m >= (RSI_OB - 5.0):
            score += 1
            reasons.append(f"15m เข้าเขต Overbought ร่วมด้วย (RSI {rsi_15m:.1f})")
        elif state_5m["vol_ratio"] >= 2.2 and rsi_15m <= 52.0:
            score += 1
            reasons.append(f"5m วอลุ่มเทขายหนาแน่น ({state_5m['vol_ratio']:.1f}x)")

    # 2. เช็คความสอดคล้องกับ 1h (คุมเทรนด์หลัก)
    aligned_1h = False
    if direction == "BULL":
        if state_1h["trend"] == "BULL":
            score += 1
            aligned_1h = True
            reasons.append("ตามเทรนด์ใหญ่ 1h (ขาขึ้น)")
        elif is_reversal and rsi_1h <= (RSI_OS + 6.0):
            score += 1
            aligned_1h = True
            reasons.append(f"1h Oversold รอเด้งใหญ่ (RSI {rsi_1h:.1f})")
    else:
        if state_1h["trend"] == "BEAR":
            score += 1
            aligned_1h = True
            reasons.append("ตามเทรนด์ใหญ่ 1h (ขาลง)")
        elif is_reversal and rsi_1h >= (RSI_OB - 6.0):
            score += 1
            aligned_1h = True
            reasons.append(f"1h Overbought รอย่อใหญ่ (RSI {rsi_1h:.1f})")

    stars = min(3, max(1, score))
    star_str = "⭐" * stars

    if stars == 3:
        grade = f"{star_str} (3/3 ดาว — A+ Setup สอดคล้องครบ 5m+15m+1h)"
    elif stars == 2:
        if aligned_1h:
            grade = f"{star_str} (2/3 ดาว — สัญญาณตามเทรนด์หลัก 1h)"
        else:
            grade = f"{star_str} (2/3 ดาว — 5m+15m ยืนยันตรงกัน / สวนเทรนด์ 1h เน้นเก็บสั้น TP1-TP2)"
    else:
        grade = f"{star_str} (1/3 ดาว — สัญญาณเดี่ยว ยังไม่ยืนยันข้าม TF)"

    return stars, grade, aligned_1h, reasons


def build_mtf_trade_plan(direction, price, state_5m, state_15m, state_1h, aligned_1h):
    """
    คำนวณจุดเข้า-ออกผสาน 3 Timeframe:
      • จุดเข้า (Entry Zone) : คัดจุดเข้าคมๆ จาก ATR ของ 5m
      • จุดตัดขาดทุน (SL)     : วางตามโครงสร้าง Swing High/Low และ ATR ของ 15m (กันไส้เทียน 5m สะบัดกิน SL)
      • เป้าทำกำไร (TP1/TP2/TP3):
          - TP1 = เป้าสั้นตามกรอบ 5m/15m (1.3R)
          - TP2 = เป้ากลางตามโครงสร้าง 15m (2.3R)
          - TP3 = เป้ารันเทรนด์ตามกรอบ 1h (3.6R หรือ ATR 1h)
    """
    if price <= 0:
        return None

    atr_5m = state_5m["atr"] if state_5m and state_5m["atr"] > 0 else price * 0.006
    atr_15m = state_15m["atr"] if state_15m and state_15m["atr"] > 0 else price * 0.012
    atr_1h = state_1h["atr"] if state_1h and state_1h["atr"] > 0 else price * 0.024

    # คำนวณระยะ SL จากโครงสร้าง 15m (Swing Low / Swing High 8 แท่งล่าสุดของ 15m + Buffer 0.25*ATR_15m)
    raw_risk = atr_15m * 1.4
    candles_15m = state_15m["candles"] if state_15m else []
    if len(candles_15m) >= 6:
        recent_15m = candles_15m[-8:] if len(candles_15m) >= 8 else candles_15m
        if direction == "BULL":
            swing_low_15m = min(c["low"] for c in recent_15m)
            swing_dist = (price - swing_low_15m) + (0.20 * atr_15m)
            if swing_dist > 0:
                raw_risk = max(raw_risk, swing_dist)
        else:
            swing_high_15m = max(c["high"] for c in recent_15m)
            swing_dist = (swing_high_15m - price) + (0.20 * atr_15m)
            if swing_dist > 0:
                raw_risk = max(raw_risk, swing_dist)

    min_risk = price * 0.007  # ขั้นต่ำ 0.70%
    max_risk = price * 0.048  # สูงสุด 4.80%
    risk = max(min_risk, min(max_risk, raw_risk))
    risk_pct = (risk / price) * 100.0

    # ระยะเป้าทำกำไร TP1 (สั้น 5m/15m), TP2 (กลาง 15m), TP3 (ใหญ่ 1h)
    tp1_dist = max(risk * 1.3, atr_5m * 2.2)
    tp2_dist = max(risk * 2.3, atr_15m * 2.2)
    tp3_dist = max(risk * 3.6, atr_1h * 1.8)

    if direction == "BULL":
        entry_low = price - (0.30 * atr_5m)
        sl = price - risk
        tp1 = price + tp1_dist
        tp2 = price + tp2_dist
        tp3 = price + tp3_dist
        tp1_pct = (tp1_dist / price) * 100.0
        tp2_pct = (tp2_dist / price) * 100.0
        tp3_pct = (tp3_dist / price) * 100.0
        side_note = "ตามเทรนด์ 1h รันเทรนด์ได้" if aligned_1h else "เล่นรอบสั้น เน้นแบ่งปิด TP1/TP2"
        return {
            "side_label": f"🟢 เปิด LONG ({side_note})",
            "entry_str": f"${fmt_price(entry_low)} - ${fmt_price(price)} (โซนรับ 5m)",
            "tp1_str": f"${fmt_price(tp1)} (+{tp1_pct:.2f}%) [เป้าสั้น 5m]",
            "tp2_str": f"${fmt_price(tp2)} (+{tp2_pct:.2f}%) [เป้ากลาง 15m]",
            "tp3_str": f"${fmt_price(tp3)} (+{tp3_pct:.2f}%) [เป้ารันเทรนด์ 1h]",
            "sl_str": f"${fmt_price(sl)} (-{risk_pct:.2f}%) [ใต้ฐาน 15m]",
            "entry_price": price,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "risk_pct": risk_pct,
        }
    else:
        entry_high = price + (0.30 * atr_5m)
        sl = price + risk
        tp1 = price - tp1_dist
        tp2 = price - tp2_dist
        tp3 = price - tp3_dist
        tp1_pct = (tp1_dist / price) * 100.0
        tp2_pct = (tp2_dist / price) * 100.0
        tp3_pct = (tp3_dist / price) * 100.0
        side_note = "ตามเทรนด์ 1h รันเทรนด์ได้" if aligned_1h else "เล่นรอบสั้น เน้นแบ่งปิด TP1/TP2"
        return {
            "side_label": f"🔴 เปิด SHORT ({side_note})",
            "entry_str": f"${fmt_price(price)} - ${fmt_price(entry_high)} (โซนต้าน 5m)",
            "tp1_str": f"${fmt_price(tp1)} (+{tp1_pct:.2f}%) [เป้าสั้น 5m]",
            "tp2_str": f"${fmt_price(tp2)} (+{tp2_pct:.2f}%) [เป้ากลาง 15m]",
            "tp3_str": f"${fmt_price(tp3)} (+{tp3_pct:.2f}%) [เป้ารันเทรนด์ 1h]",
            "sl_str": f"${fmt_price(sl)} (-{risk_pct:.2f}%) [เหนือต้าน 15m]",
            "entry_price": price,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "risk_pct": risk_pct,
        }


CSV_HEADER = "timestamp,trade_id,event,symbol,side,stars,timeframe,signal,entry_price,exec_price,closed_pct,margin_used,leverage,gross_pnl_usdt,fee_usdt,net_pnl_usdt,balance_after_usdt,equity_after_usdt,note\n"


def send_paper_webhook(title, symbol, lines_th, discord_color):
    if not PAPER_NOTIFY_WEBHOOKS:
        return

    futures_url = f"https://www.binance.com/en/futures/{symbol}"

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        msg_lines = [f"<b>{html.escape(title)}</b>"]
        for line in lines_th:
            msg_lines.append(html.escape(str(line)))
        msg_lines.append(f'\n🔗 <a href="{futures_url}">ดูกราฟ {html.escape(symbol)}</a>')
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": "\n".join(msg_lines),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if PAPER_TELEGRAM_BOT_ENABLED:
            payload["reply_markup"] = {
                "inline_keyboard": [
                    [
                        {"text": "💼 ดูพอร์ต + ปุ่มสั่งการ", "callback_data": "paper:panel"},
                        {"text": "📜 ประวัติ 10 รายการ", "callback_data": "paper:history_new"},
                    ]
                ]
            }
        try:
            http_post_json(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                payload,
            )
        except Exception as e:
            print(f"   ⚠️ ส่ง Telegram (Paper) ไม่สำเร็จ: {e}")

    if DISCORD_WEBHOOK_URL:
        try:
            http_post_json(
                DISCORD_WEBHOOK_URL,
                {
                    "username": "Scanner-f Paper Trader",
                    "embeds": [
                        {
                            "title": title,
                            "url": futures_url,
                            "description": "\n".join(lines_th),
                            "color": discord_color,
                            "footer": {"text": "Scanner-f Paper Trading Simulator"},
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                    ],
                },
            )
        except Exception as e:
            print(f"   ⚠️ ส่ง Discord (Paper) ไม่สำเร็จ: {e}")


def fetch_all_prices():
    rows = http_get_json(f"{FAPI_URL}/fapi/v1/ticker/price", timeout=10)
    pmap = {}
    for r in rows:
        sym = r.get("symbol")
        try:
            p = float(r.get("price", 0.0))
        except Exception:
            p = 0.0
        if sym and p > 0:
            pmap[sym] = p
    return pmap


def tg_api_post(method, payload, timeout=12):
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN not configured")
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "scanner-f-mtf-mobile/2.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        err_body = ""
        try:
            err_body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            pass
        raise RuntimeError(f"HTTP {e.code}: {err_body or e.reason}") from e


def tg_send_html(html_text, inline_keyboard=None):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return 0
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": html_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if inline_keyboard is not None:
        payload["reply_markup"] = {"inline_keyboard": inline_keyboard}
    res = tg_api_post("sendMessage", payload)
    return res.get("result", {}).get("message_id", 0)


def tg_edit_html(chat_id, message_id, html_text, inline_keyboard=None):
    if not TELEGRAM_BOT_TOKEN:
        return
    payload = {
        "chat_id": str(chat_id),
        "message_id": int(message_id),
        "text": html_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if inline_keyboard is not None:
        payload["reply_markup"] = {"inline_keyboard": inline_keyboard}
    try:
        tg_api_post("editMessageText", payload)
    except Exception as e:
        if "message is not modified" in str(e):
            return
        raise


def tg_answer_callback(callback_query_id, text=None, show_alert=False):
    if not TELEGRAM_BOT_TOKEN or not callback_query_id:
        return
    payload = {"callback_query_id": callback_query_id, "show_alert": bool(show_alert)}
    if text:
        payload["text"] = str(text)
    try:
        tg_api_post("answerCallbackQuery", payload)
    except Exception:
        pass


def tg_set_commands():
    if not TELEGRAM_BOT_TOKEN:
        return
    payload = {
        "commands": [
            {"command": "portfolio", "description": "💼 ดูพอร์ตจำลอง + ปุ่มสั่งการ"},
            {"command": "settings", "description": "⚙️ ตั้งค่า Max ไม้ / ขนาด% / Leverage / ดาว"},
            {"command": "set", "description": "🎛️ ตั้งค่าพอร์ตแบบพิมพ์เอง เช่น /set lev 15"},
            {"command": "history", "description": "📜 ดูประวัติเทรด 10 รายการล่าสุด"},
            {"command": "scan", "description": "🔍 สแกนตลาด MTF ทันที 1 รอบ"},
            {"command": "pause", "description": "⏸️ พักการเปิดออเดอร์ใหม่ชั่วคราว"},
            {"command": "resume", "description": "▶️ เปิดรับออเดอร์จำลองใหม่อีกครั้ง"},
            {"command": "closeall", "description": "❌ ปิดออเดอร์จำลองทั้งหมดทันที"},
            {"command": "reset", "description": "🔄 รีเซ็ตพอร์ตจำลองกลับเป็นทุนตั้งต้น"},
            {"command": "help", "description": "❓ ดูคำสั่งทั้งหมดของบอท"},
        ]
    }
    try:
        tg_api_post("setMyCommands", payload)
    except Exception:
        pass


class PaperTrader:
    def __init__(self, state_file=None, csv_file=None):
        self.state_file = state_file or PAPER_STATE_FILE
        self.csv_file = csv_file or PAPER_TRADES_CSV
        self._lock = threading.RLock()
        self.state = self._load_or_init()

    def _new_state(self):
        return {
            "initial_balance": PAPER_INITIAL_BALANCE,
            "free_balance": PAPER_INITIAL_BALANCE,
            "realized_pnl": 0.0,
            "total_fees": 0.0,
            "peak_equity": PAPER_INITIAL_BALANCE,
            "max_drawdown_pct": 0.0,
            "next_trade_id": 1,
            "total_closed": 0,
            "win_count": 0,
            "loss_count": 0,
            "paused_opening": False,
            "custom_leverage": None,
            "custom_position_pct": None,
            "custom_max_open_positions": None,
            "custom_min_stars": None,
            "open_positions": [],
        }

    def _load_or_init(self):
        base = self._new_state()
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict) and "free_balance" in data:
                        base.update(data)
                        if not isinstance(base.get("open_positions"), list):
                            base["open_positions"] = []
                        base.setdefault("paused_opening", False)
                        base.setdefault("custom_leverage", None)
                        base.setdefault("custom_position_pct", None)
                        base.setdefault("custom_max_open_positions", None)
                        base.setdefault("custom_min_stars", None)
                        return base
            except Exception:
                pass
        return base

    def save_state(self):
        with self._lock:
            try:
                with open(self.state_file, "w", encoding="utf-8") as f:
                    json.dump(self.state, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print(f"⚠️ บันทึกพอร์ตจำลองไม่สำเร็จ: {e}")

    def get_state_snapshot(self):
        with self._lock:
            return copy.deepcopy(self.state)

    def set_paused(self, paused):
        with self._lock:
            self.state["paused_opening"] = bool(paused)
            self.save_state()
            return copy.deepcopy(self.state)

    def update_runtime_settings(self, max_open_positions=None, position_pct=None, leverage=None, min_stars=None):
        with self._lock:
            if max_open_positions is not None:
                self.state["custom_max_open_positions"] = max(1, min(30, int(max_open_positions)))
            if position_pct is not None:
                val = float(position_pct)
                if math.isfinite(val):
                    self.state["custom_position_pct"] = max(1.0, min(100.0, val))
            if leverage is not None:
                val = float(leverage)
                if math.isfinite(val):
                    self.state["custom_leverage"] = max(1.0, min(125.0, val))
            if min_stars is not None:
                self.state["custom_min_stars"] = max(1, min(3, int(min_stars)))
            self.save_state()
            return copy.deepcopy(self.state)

    def clear_runtime_settings(self):
        with self._lock:
            self.state["custom_leverage"] = None
            self.state["custom_position_pct"] = None
            self.state["custom_max_open_positions"] = None
            self.state["custom_min_stars"] = None
            self.save_state()
            return copy.deepcopy(self.state)

    def reset_portfolio(self):
        with self._lock:
            custom_keys = {
                k: self.state.get(k)
                for k in (
                    "custom_leverage",
                    "custom_position_pct",
                    "custom_max_open_positions",
                    "custom_min_stars",
                )
            }
            fresh = self._new_state()
            fresh.update(custom_keys)
            self.state = fresh
            self.save_state()
            if os.path.exists(self.csv_file):
                try:
                    os.remove(self.csv_file)
                except Exception:
                    pass
            return copy.deepcopy(self.state)

    @staticmethod
    def state_effective_leverage(st):
        raw = st.get("custom_leverage")
        val = float(raw) if isinstance(raw, (int, float)) and math.isfinite(float(raw)) else float(PAPER_LEVERAGE)
        return max(1.0, min(125.0, val))

    @staticmethod
    def state_effective_position_pct(st):
        raw = st.get("custom_position_pct")
        val = float(raw) if isinstance(raw, (int, float)) and math.isfinite(float(raw)) else float(PAPER_POSITION_PCT)
        return max(1.0, min(100.0, val))

    @staticmethod
    def state_effective_max_open_positions(st):
        raw = st.get("custom_max_open_positions")
        val = int(raw) if isinstance(raw, (int, float)) else int(PAPER_MAX_OPEN_POSITIONS)
        return max(1, min(30, val))

    @staticmethod
    def state_effective_min_stars(st):
        raw = st.get("custom_min_stars")
        val = int(raw) if isinstance(raw, (int, float)) else int(PAPER_MIN_STARS)
        return max(1, min(3, val))

    @staticmethod
    def state_has_custom_settings(st):
        return any(
            st.get(k) is not None
            for k in (
                "custom_leverage",
                "custom_position_pct",
                "custom_max_open_positions",
                "custom_min_stars",
            )
        )

    def recent_csv_events(self, limit=10):
        with self._lock:
            if not os.path.exists(self.csv_file):
                return []
            try:
                with open(self.csv_file, "r", encoding="utf-8") as f:
                    lines = f.read().splitlines()
            except Exception:
                return []
        rows = []
        for line in lines[1:]:
            cols = line.split(",")
            if len(cols) < 16:
                continue
            try:
                exec_price = float(cols[9])
            except Exception:
                exec_price = 0.0
            try:
                net_pnl = float(cols[15])
            except Exception:
                net_pnl = 0.0
            rows.append(
                {
                    "timestamp": cols[0],
                    "trade_id": cols[1],
                    "event": cols[2],
                    "symbol": cols[3],
                    "side": cols[4],
                    "exec_price": exec_price,
                    "net_pnl": net_pnl,
                }
            )
        if len(rows) > limit:
            rows = rows[-limit:]
        rows.reverse()
        return rows

    @staticmethod
    def pos_unrealized_pnl(pos):
        if pos["side"] == "Long":
            return (pos["last_price"] - pos["entry_price"]) * pos["remaining_qty"]
        return (pos["entry_price"] - pos["last_price"]) * pos["remaining_qty"]

    @staticmethod
    def pos_unrealized_roe_pct(pos):
        if pos["remaining_margin"] <= 0:
            return 0.0
        return (PaperTrader.pos_unrealized_pnl(pos) / pos["remaining_margin"]) * 100.0

    @staticmethod
    def state_locked_margin(st):
        return sum(p["remaining_margin"] for p in st.get("open_positions", []))

    @staticmethod
    def state_unrealized_pnl(st):
        return sum(PaperTrader.pos_unrealized_pnl(p) for p in st.get("open_positions", []))

    @staticmethod
    def state_total_equity(st):
        return st["free_balance"] + PaperTrader.state_locked_margin(st) + PaperTrader.state_unrealized_pnl(st)

    @staticmethod
    def state_win_rate_pct(st):
        total = st.get("total_closed", 0)
        if total == 0:
            return 0.0
        return (st.get("win_count", 0) / total) * 100.0

    def locked_margin(self):
        with self._lock:
            return self.state_locked_margin(self.state)

    def unrealized_pnl(self):
        with self._lock:
            return self.state_unrealized_pnl(self.state)

    def total_equity(self):
        with self._lock:
            return self.state_total_equity(self.state)

    def win_rate_pct(self):
        with self._lock:
            return self.state_win_rate_pct(self.state)

    def update_drawdown(self):
        eq = self.total_equity()
        if eq > self.state["peak_equity"]:
            self.state["peak_equity"] = eq
        peak = self.state["peak_equity"]
        if peak > 0:
            dd = ((peak - eq) / peak) * 100.0
            if dd > self.state["max_drawdown_pct"]:
                self.state["max_drawdown_pct"] = dd

    def append_csv(self, trade_id, event, pos, exec_price, closed_pct, gross_pnl, fee, net_pnl, free_after, eq_after, note):
        needs_header = (not os.path.exists(self.csv_file)) or os.path.getsize(self.csv_file) == 0
        try:
            with open(self.csv_file, "a", encoding="utf-8") as f:
                if needs_header:
                    f.write(CSV_HEADER)
                ts = datetime.now(timezone.utc).isoformat()
                side_str = "LONG" if pos["side"] == "Long" else "SHORT"
                safe_note = str(note).replace(",", ";")
                f.write(
                    f"{ts},{trade_id},{event},{pos['symbol']},{side_str},{pos['stars']},{pos['timeframe']},"
                    f"{pos['signal_category']},{pos['entry_price']:.8f},{exec_price:.8f},{closed_pct:.1f},"
                    f"{pos['initial_margin']:.4f},{pos['leverage']:.1f},{gross_pnl:.4f},{fee:.4f},{net_pnl:.4f},"
                    f"{free_after:.4f},{eq_after:.4f},{safe_note}\n"
                )
        except Exception as e:
            print(f"⚠️ บันทึกไฟล์ {self.csv_file} ไม่สำเร็จ: {e}")

    def try_open_position(self, symbol, direction, category_key, timeframe, stars, plan, notify=True):
        if not PAPER_TRADING_ENABLED or not plan:
            return None
        if direction not in ("BULL", "BEAR"):
            return None
        entry_price = plan["entry_price"]
        if entry_price <= 0:
            return None

        pending_notif = None
        with self._lock:
            if self.state.get("paused_opening"):
                return None
            eff_min_stars = self.state_effective_min_stars(self.state)
            if stars < eff_min_stars:
                return None
            eff_max_pos = self.state_effective_max_open_positions(self.state)
            if len(self.state["open_positions"]) >= eff_max_pos:
                return None
            if any(p["symbol"] == symbol for p in self.state["open_positions"]):
                return None

            eff_pos_pct = self.state_effective_position_pct(self.state)
            eff_leverage = self.state_effective_leverage(self.state)

            side = "Long" if direction == "BULL" else "Short"
            equity = self.total_equity()
            desired_margin = equity * (eff_pos_pct / 100.0)
            fee_rate = PAPER_TAKER_FEE_PCT / 100.0
            max_affordable_margin = self.state["free_balance"] / (1.0 + eff_leverage * fee_rate)
            margin = min(desired_margin, max_affordable_margin)
            if margin < 5.0:
                return None

            notional = margin * eff_leverage
            qty = notional / entry_price
            entry_fee = notional * fee_rate

            self.state["free_balance"] -= (margin + entry_fee)
            self.state["realized_pnl"] -= entry_fee
            self.state["total_fees"] += entry_fee

            trade_id = self.state["next_trade_id"]
            self.state["next_trade_id"] += 1

            pos = {
                "id": trade_id,
                "symbol": symbol,
                "side": side,
                "stars": stars,
                "signal_category": category_key,
                "timeframe": timeframe,
                "entry_price": entry_price,
                "initial_sl": plan["sl"],
                "current_sl": plan["sl"],
                "tp1": plan["tp1"],
                "tp2": plan["tp2"],
                "tp3": plan["tp3"],
                "leverage": eff_leverage,
                "initial_margin": margin,
                "remaining_margin": margin,
                "initial_qty": qty,
                "remaining_qty": qty,
                "remaining_fraction": 1.0,
                "tp1_hit": False,
                "tp2_hit": False,
                "realized_pnl_trade": -entry_fee,
                "fees_paid_trade": entry_fee,
                "last_price": entry_price,
                "opened_at": datetime.now(timezone.utc).isoformat(),
            }

            self.state["open_positions"].append(pos)
            self.update_drawdown()
            eq_after = self.total_equity()
            free_after = self.state["free_balance"]

            self.append_csv(
                trade_id,
                "OPEN",
                pos,
                entry_price,
                0.0,
                0.0,
                entry_fee,
                -entry_fee,
                free_after,
                eq_after,
                f"{category_key} ({stars}*)",
            )
            self.save_state()

            side_emoji = "🟢 LONG" if side == "Long" else "🔴 SHORT"
            title = f"🧪 [PAPER] เปิดไม้ #{trade_id} {side_emoji} {symbol}"
            lines_th = [
                f"⭐ สัญญาณ: {category_key} ({stars} ดาว | TF: {timeframe})",
                f"💰 Margin: ${margin:.2f} ({fmt_num_clean(eff_leverage)}x | มูลค่าสัญญา ${notional:.2f})",
                f"🎯 ราคาเข้า (Entry): ${fmt_price(entry_price)}",
                f"📊 เป้าปิด 3 ส่วน: TP1 ${fmt_price(plan['tp1'])} (40%) | TP2 ${fmt_price(plan['tp2'])} (30%) | TP3 ${fmt_price(plan['tp3'])} (30%)",
                f"🛡️ Stop Loss: ${fmt_price(plan['sl'])} (-{plan['risk_pct']:.2f}%) | ค่าธรรมเนียม: ${entry_fee:.2f}",
                f"💼 พอร์ตปัจจุบัน: Equity ${eq_after:.2f} | ถืออยู่ {len(self.state['open_positions'])}/{eff_max_pos} ไม้",
            ]
            pending_notif = (title, symbol, lines_th, 0x00E676 if side == "Long" else 0xFF5252)

        print(f"\n   {pending_notif[0]}")
        for ln in pending_notif[2]:
            print(f"      {ln}")
        if notify:
            send_paper_webhook(*pending_notif)

        return trade_id

    def update_prices(self, price_map, notify=True):
        if not PAPER_TRADING_ENABLED or not price_map:
            return

        notifications = []
        with self._lock:
            if not self.state["open_positions"]:
                return

            fee_rate = PAPER_TAKER_FEE_PCT / 100.0
            idx = 0
            while idx < len(self.state["open_positions"]):
                pos = self.state["open_positions"][idx]
                sym = pos["symbol"]
                price = price_map.get(sym, 0.0)
                if price <= 0:
                    idx += 1
                    continue

                pos["last_price"] = price
                is_long = pos["side"] == "Long"

                # 1. เช็ค Stop Loss
                sl_hit = (price <= pos["current_sl"]) if is_long else (price >= pos["current_sl"])
                if sl_hit:
                    close_qty = pos["remaining_qty"]
                    exec_price = price
                    gross = ((exec_price - pos["entry_price"]) if is_long else (pos["entry_price"] - exec_price)) * close_qty
                    exit_fee = (close_qty * exec_price) * fee_rate
                    raw_net = gross - exit_fee
                    returned_cash = max(0.0, pos["remaining_margin"] + raw_net)
                    net_step = returned_cash - pos["remaining_margin"]

                    pos["realized_pnl_trade"] += net_step
                    pos["fees_paid_trade"] += exit_fee
                    pos["remaining_qty"] = 0.0
                    pos["remaining_margin"] = 0.0
                    pos["remaining_fraction"] = 0.0

                    self.state["free_balance"] += returned_cash
                    self.state["realized_pnl"] += net_step
                    self.state["total_fees"] += exit_fee
                    self.state["total_closed"] += 1
                    if pos["realized_pnl_trade"] >= 0:
                        self.state["win_count"] += 1
                    else:
                        self.state["loss_count"] += 1

                    self.state["open_positions"].pop(idx)
                    self.update_drawdown()
                    eq_after = self.total_equity()
                    free_after = self.state["free_balance"]

                    if pos["tp2_hit"]:
                        reason = "TRAILING_SL_TP1"
                        label = "Trailing SL @ TP1 (ล็อกกำไรสำเร็จ)"
                    elif pos["tp1_hit"]:
                        reason = "BREAKEVEN_SL"
                        label = "Breakeven SL (กันทุนหลังชน TP1)"
                    else:
                        reason = "STOP_LOSS"
                        label = "Stop Loss (ตัดขาดทุนตามแผน)"

                    pct_closed = (close_qty / pos["initial_qty"]) * 100.0 if pos["initial_qty"] > 0 else 100.0
                    self.append_csv(pos["id"], reason, pos, exec_price, pct_closed, gross, exit_fee, net_step, free_after, eq_after, reason)

                    icon = "✅" if pos["realized_pnl_trade"] >= 0 else "🛑"
                    side_str = "LONG" if is_long else "SHORT"
                    title = f"{icon} [PAPER] ปิดไม้ #{pos['id']} {sym} — {label}"
                    lines_th = [
                        f"📌 สถานะ: {side_str} ({label})",
                        f"🎯 Entry: ${fmt_price(pos['entry_price'])} -> Exit: ${fmt_price(exec_price)}",
                        f"💵 กำไร/ขาดทุนสุทธิไม้นี้: {pos['realized_pnl_trade']:+.2f} USDT (ค่าธรรมเนียมรวม ${pos['fees_paid_trade']:.2f})",
                        f"📊 สถิติพอร์ต: Equity ${eq_after:.2f} | Win Rate {self.win_rate_pct():.1f}% ({self.state['win_count']}W/{self.state['loss_count']}L) | Max DD {self.state['max_drawdown_pct']:.2f}%",
                    ]
                    notifications.append((title, sym, lines_th, 0x00E676 if pos["realized_pnl_trade"] >= 0 else 0xFF5252))
                    continue

                # 2. เช็ค TP1 (ปิด 40% + เลื่อน SL กันทุน)
                tp1_hit_now = (not pos["tp1_hit"]) and ((price >= pos["tp1"]) if is_long else (price <= pos["tp1"]))
                if tp1_hit_now:
                    frac = 0.40
                    close_qty = pos["initial_qty"] * frac
                    margin_released = pos["initial_margin"] * frac
                    exec_price = pos["tp1"]
                    gross = ((exec_price - pos["entry_price"]) if is_long else (pos["entry_price"] - exec_price)) * close_qty
                    exit_fee = (close_qty * exec_price) * fee_rate
                    net_step = gross - exit_fee

                    pos["tp1_hit"] = True
                    pos["remaining_qty"] = max(0.0, pos["remaining_qty"] - close_qty)
                    pos["remaining_margin"] = max(0.0, pos["remaining_margin"] - margin_released)
                    pos["remaining_fraction"] = max(0.0, pos["remaining_fraction"] - frac)
                    pos["realized_pnl_trade"] += net_step
                    pos["fees_paid_trade"] += exit_fee

                    be_buffer = fee_rate * 2.2
                    pos["current_sl"] = pos["entry_price"] * (1.0 + be_buffer) if is_long else pos["entry_price"] * (1.0 - be_buffer)

                    self.state["free_balance"] += (margin_released + net_step)
                    self.state["realized_pnl"] += net_step
                    self.state["total_fees"] += exit_fee
                    self.update_drawdown()
                    eq_after = self.total_equity()
                    free_after = self.state["free_balance"]

                    self.append_csv(pos["id"], "TP1_PARTIAL", pos, exec_price, 40.0, gross, exit_fee, net_step, free_after, eq_after, "Closed 40% & Moved SL to Breakeven")
                    title = f"💰 [PAPER] TP1 แตก! #{pos['id']} {sym} (+ปิด 40%)"
                    lines_th = [
                        f"🎯 ชนเป้า TP1 ที่ ${fmt_price(exec_price)} (เก็บกำไร 40% = {net_step:+.2f} USDT)",
                        f"🛡️ เลื่อน SL กันทุน (Breakeven) อัตโนมัติ -> ${fmt_price(pos['current_sl'])}",
                        f"💼 เหลือถือรันต่อ 60% ลุ้น TP2 (${fmt_price(pos['tp2'])}) / TP3 (${fmt_price(pos['tp3'])}) | Equity ${eq_after:.2f}",
                    ]
                    notifications.append((title, sym, lines_th, 0x00E676))

                # 3. เช็ค TP2 (ปิด 30% + เลื่อน SL ล็อกกำไรที่ TP1)
                tp2_hit_now = pos["tp1_hit"] and (not pos["tp2_hit"]) and ((price >= pos["tp2"]) if is_long else (price <= pos["tp2"]))
                if tp2_hit_now:
                    frac = 0.30
                    close_qty = pos["initial_qty"] * frac
                    margin_released = pos["initial_margin"] * frac
                    exec_price = pos["tp2"]
                    gross = ((exec_price - pos["entry_price"]) if is_long else (pos["entry_price"] - exec_price)) * close_qty
                    exit_fee = (close_qty * exec_price) * fee_rate
                    net_step = gross - exit_fee

                    pos["tp2_hit"] = True
                    pos["remaining_qty"] = max(0.0, pos["remaining_qty"] - close_qty)
                    pos["remaining_margin"] = max(0.0, pos["remaining_margin"] - margin_released)
                    pos["remaining_fraction"] = max(0.0, pos["remaining_fraction"] - frac)
                    pos["realized_pnl_trade"] += net_step
                    pos["fees_paid_trade"] += exit_fee
                    pos["current_sl"] = pos["tp1"]

                    self.state["free_balance"] += (margin_released + net_step)
                    self.state["realized_pnl"] += net_step
                    self.state["total_fees"] += exit_fee
                    self.update_drawdown()
                    eq_after = self.total_equity()
                    free_after = self.state["free_balance"]

                    self.append_csv(pos["id"], "TP2_PARTIAL", pos, exec_price, 30.0, gross, exit_fee, net_step, free_after, eq_after, "Closed 30% & Trailed SL to TP1")
                    title = f"🚀 [PAPER] TP2 แตก! #{pos['id']} {sym} (+ปิดอีก 30%)"
                    lines_th = [
                        f"🎯 ชนเป้า TP2 ที่ ${fmt_price(exec_price)} (เก็บกำไรเพิ่ม 30% = {net_step:+.2f} USDT)",
                        f"🔒 เลื่อน Trailing SL ล็อกกำไรที่ TP1 -> ${fmt_price(pos['current_sl'])}",
                        f"💼 เหลือถือรันต่อ 30% ลุ้น TP3 (${fmt_price(pos['tp3'])}) | Equity ${eq_after:.2f}",
                    ]
                    notifications.append((title, sym, lines_th, 0x00E676))

                # 4. เช็ค TP3 (ปิด 30% สุดท้าย)
                tp3_hit_now = pos["tp2_hit"] and ((price >= pos["tp3"]) if is_long else (price <= pos["tp3"]))
                if tp3_hit_now:
                    close_qty = pos["remaining_qty"]
                    margin_released = pos["remaining_margin"]
                    exec_price = pos["tp3"]
                    gross = ((exec_price - pos["entry_price"]) if is_long else (pos["entry_price"] - exec_price)) * close_qty
                    exit_fee = (close_qty * exec_price) * fee_rate
                    net_step = gross - exit_fee

                    pos["realized_pnl_trade"] += net_step
                    pos["fees_paid_trade"] += exit_fee
                    pos["remaining_qty"] = 0.0
                    pos["remaining_margin"] = 0.0
                    pos["remaining_fraction"] = 0.0

                    self.state["free_balance"] += (margin_released + net_step)
                    self.state["realized_pnl"] += net_step
                    self.state["total_fees"] += exit_fee
                    self.state["total_closed"] += 1
                    if pos["realized_pnl_trade"] >= 0:
                        self.state["win_count"] += 1
                    else:
                        self.state["loss_count"] += 1

                    self.state["open_positions"].pop(idx)
                    self.update_drawdown()
                    eq_after = self.total_equity()
                    free_after = self.state["free_balance"]

                    self.append_csv(pos["id"], "TP3_FULL_CLOSE", pos, exec_price, 30.0, gross, exit_fee, net_step, free_after, eq_after, "Hit TP3 Full Take Profit")
                    title = f"🏆 [PAPER] ปิดกำไรครบ 3 เป้า! #{pos['id']} {sym}"
                    lines_th = [
                        f"🎯 ชนเป้า TP3 ที่ ${fmt_price(exec_price)} (ปิด 30% สุดท้าย = {net_step:+.2f} USDT)",
                        f"💎 กำไรรวมสุทธิทั้งไม้: {pos['realized_pnl_trade']:+.2f} USDT (ค่าธรรมเนียมรวม ${pos['fees_paid_trade']:.2f})",
                        f"📊 สถิติพอร์ต: Equity ${eq_after:.2f} | Win Rate {self.win_rate_pct():.1f}% ({self.state['win_count']}W/{self.state['loss_count']}L) | Max DD {self.state['max_drawdown_pct']:.2f}%",
                    ]
                    notifications.append((title, sym, lines_th, 0x00E676))
                    continue

                idx += 1

            self.update_drawdown()
            self.save_state()

        for title, sym, lines_th, color in notifications:
            print(f"\n   {title}")
            for ln in lines_th:
                print(f"      {ln}")
            if notify:
                send_paper_webhook(title, sym, lines_th, color)

    def close_position_by_id(self, trade_id, market_prices=None, notify=True):
        notif = None
        res = None
        with self._lock:
            idx = next((i for i, p in enumerate(self.state["open_positions"]) if p["id"] == trade_id), None)
            if idx is None:
                return None
            pos = self.state["open_positions"].pop(idx)
            sym = pos["symbol"]
            exec_price = float(market_prices.get(sym, 0.0)) if market_prices else 0.0
            if exec_price <= 0:
                exec_price = pos["last_price"]
            pos["last_price"] = exec_price

            is_long = pos["side"] == "Long"
            fee_rate = PAPER_TAKER_FEE_PCT / 100.0
            close_qty = pos["remaining_qty"]
            gross = ((exec_price - pos["entry_price"]) if is_long else (pos["entry_price"] - exec_price)) * close_qty
            exit_fee = (close_qty * exec_price) * fee_rate
            raw_net = gross - exit_fee
            returned_cash = max(0.0, pos["remaining_margin"] + raw_net)
            net_step = returned_cash - pos["remaining_margin"]

            pos["realized_pnl_trade"] += net_step
            pos["fees_paid_trade"] += exit_fee
            pos["remaining_qty"] = 0.0
            pos["remaining_margin"] = 0.0
            pos["remaining_fraction"] = 0.0

            self.state["free_balance"] += returned_cash
            self.state["realized_pnl"] += net_step
            self.state["total_fees"] += exit_fee
            self.state["total_closed"] += 1
            if pos["realized_pnl_trade"] >= 0:
                self.state["win_count"] += 1
            else:
                self.state["loss_count"] += 1

            self.update_drawdown()
            eq_after = self.total_equity()
            free_after = self.state["free_balance"]
            pct_closed = (close_qty / pos["initial_qty"]) * 100.0 if pos["initial_qty"] > 0 else 100.0
            self.append_csv(pos["id"], "MANUAL_CLOSE", pos, exec_price, pct_closed, gross, exit_fee, net_step, free_after, eq_after, "Closed manually via Telegram")
            self.save_state()

            icon = "✅" if pos["realized_pnl_trade"] >= 0 else "🛑"
            side_str = "LONG" if is_long else "SHORT"
            title = f"{icon} [PAPER] ปิดมือ #{pos['id']} {sym} — สั่งปิดจาก Telegram"
            lines_th = [
                f"📌 สถานะ: {side_str} (ปิดด้วยตนเอง)",
                f"🎯 Entry: ${fmt_price(pos['entry_price'])} -> Exit: ${fmt_price(exec_price)}",
                f"💵 กำไร/ขาดทุนสุทธิไม้นี้: {pos['realized_pnl_trade']:+.2f} USDT (ค่าธรรมเนียมรวม ${pos['fees_paid_trade']:.2f})",
                f"📊 สถิติพอร์ต: Equity ${eq_after:.2f} | Win Rate {self.win_rate_pct():.1f}% ({self.state['win_count']}W/{self.state['loss_count']}L)",
            ]
            notif = (title, sym, lines_th, 0x00E676 if pos["realized_pnl_trade"] >= 0 else 0xFF5252)
            res = (sym, pos["realized_pnl_trade"])

        print(f"\n   {notif[0]}")
        for ln in notif[2]:
            print(f"      {ln}")
        if notify:
            send_paper_webhook(*notif)
        return res

    def close_all_positions(self, market_prices=None, notify=True):
        with self._lock:
            ids = [p["id"] for p in self.state["open_positions"]]
        cnt = 0
        sum_pnl = 0.0
        for tid in ids:
            res = self.close_position_by_id(tid, market_prices=market_prices, notify=notify)
            if res is not None:
                cnt += 1
                sum_pnl += res[1]
        return cnt, sum_pnl

    def print_portfolio_summary(self):
        st = self.get_state_snapshot()
        eq = self.state_total_equity(st)
        init_bal = st["initial_balance"]
        net_pct = ((eq - init_bal) / init_bal * 100.0) if init_bal > 0 else 0.0
        eff_lev = self.state_effective_leverage(st)
        eff_pct = self.state_effective_position_pct(st)
        eff_max = self.state_effective_max_open_positions(st)
        eff_stars = self.state_effective_min_stars(st)
        custom_tag = " [ปรับผ่าน Telegram]" if self.state_has_custom_settings(st) else ""
        pause_txt = " [พักเปิดไม้ใหม่ ⏸️]" if st.get("paused_opening") else ""
        print("\n--------------------------------------------------------------")
        print(f"💼 พอร์ตจำลอง (Paper Trading){pause_txt} | Equity: ${eq:.2f} ({net_pct:+.2f}%) | ทุนตั้งต้น: ${init_bal:.2f}")
        print(
            f"   • ตั้งค่าไม้: {fmt_num_clean(eff_lev)}x | ไม้ละ {fmt_num_clean(eff_pct)}% | Max {eff_max} ไม้ | >= {eff_stars} ดาว{custom_tag}"
        )
        print(f"   • เงินว่าง: ${st['free_balance']:.2f} | Margin ถืออยู่: ${self.state_locked_margin(st):.2f} | uPnL: {self.state_unrealized_pnl(st):+.2f} USDT")
        print(f"   • ปิดไม้แล้ว: {st['total_closed']} | Win Rate: {self.state_win_rate_pct(st):.1f}% ({st['win_count']}W/{st['loss_count']}L) | Realized: {st['realized_pnl']:+.2f} | Max DD: {st['max_drawdown_pct']:.2f}%")
        if not st["open_positions"]:
            print(f"   • ออเดอร์ที่ถืออยู่: ไม่มี (รอสัญญาณ >= {eff_stars} ดาว)")
        else:
            print(f"   • ออเดอร์ที่ถืออยู่ ({len(st['open_positions'])}/{eff_max}):")
            for p in st["open_positions"]:
                upnl = self.pos_unrealized_pnl(p)
                uroe = self.pos_unrealized_roe_pct(p)
                side_str = "🟢 LONG" if p["side"] == "Long" else "🔴 SHORT"
                stage = "ผ่าน TP1+TP2 (30%)" if p["tp2_hit"] else ("ผ่าน TP1+กันทุน (60%)" if p["tp1_hit"] else "รอ TP1 (100%)")
                print(
                    f"     #{p['id']} {side_str} {p['symbol']} | Entry ${fmt_price(p['entry_price'])} -> ${fmt_price(p['last_price'])} | "
                    f"uPnL {upnl:+.2f} ({uroe:+.1f}%) | SL ${fmt_price(p['current_sl'])} | {stage}"
                )
        print("--------------------------------------------------------------")


paper_trader = PaperTrader()
scan_now_event = threading.Event()
scan_in_progress_lock = threading.Lock()


def fmt_num_clean(val):
    if abs(val - round(val)) < 1e-6:
        return f"{round(val):.0f}"
    return f"{val:.2f}".rstrip("0").rstrip(".")


def parse_set_command_args(args):
    tokens = []
    for raw in str(args or "").split():
        if "=" in raw:
            k, v = raw.split("=", 1)
            if k and v:
                tokens.extend([k, v])
                continue
        elif ":" in raw:
            k, v = raw.split(":", 1)
            if k and v:
                tokens.extend([k, v])
                continue
        tokens.append(raw)

    if not tokens:
        return ("error", "กรุณาระบุค่าที่ต้องการ เช่น /set lev 15 หรือ /set max 8 size 12")

    if len(tokens) == 1 and tokens[0].strip().lower() in ("default", "reset", ".env", "ค่าเริ่มต้น"):
        return ("reset", None)

    updates = {
        "max_open_positions": None,
        "position_pct": None,
        "leverage": None,
        "min_stars": None,
    }
    updated_any = False
    i = 0
    while i < len(tokens):
        raw_key = tokens[i].strip()
        key = raw_key.lstrip("-").lower()
        if key in ("default", "reset", ".env", "ค่าเริ่มต้น"):
            return ("reset", None)
        if i + 1 >= len(tokens):
            return ("error", f"ยังไม่ได้ใส่ตัวเลขหลัง `{raw_key}` (เช่น /set {raw_key} 10)")
        raw_val = tokens[i + 1].strip()

        if key in ("max", "maxpos", "max_pos", "max_positions", "positions", "pos", "limit", "ไม้"):
            cleaned = raw_val.removesuffix("ไม้").strip() if hasattr(raw_val, "removesuffix") else (raw_val[:-3].strip() if raw_val.endswith("ไม้") else raw_val)
            try:
                parsed = int(cleaned)
            except Exception:
                return ("error", f"จำนวนไม้ `{raw_val}` ไม่ถูกต้อง (ต้องเป็นเลข 1–30 เช่น /set max 8)")
            if parsed <= 0:
                return ("error", "จำนวนไม้สูงสุดต้องมากกว่า 0 (1–30 ไม้)")
            updates["max_open_positions"] = max(1, min(30, parsed))
            updated_any = True
        elif key in ("size", "pct", "margin", "pos_pct", "position_pct", "percent", "ขนาด"):
            cleaned = raw_val.rstrip("%").strip()
            try:
                parsed = float(cleaned)
            except Exception:
                return ("error", f"ขนาดไม้ `{raw_val}` ไม่ถูกต้อง (ต้องเป็นเลข 1–100 เช่น /set size 12)")
            if not math.isfinite(parsed) or parsed <= 0:
                return ("error", "ขนาดต่อไม้ (%) ต้องมากกว่า 0 (1–100%)")
            updates["position_pct"] = max(1.0, min(100.0, parsed))
            updated_any = True
        elif key in ("lev", "leverage", "x", "คูณ"):
            cleaned = raw_val.rstrip("xX").strip()
            try:
                parsed = float(cleaned)
            except Exception:
                return ("error", f"Leverage `{raw_val}` ไม่ถูกต้อง (ต้องเป็นเลข 1–125 เช่น /set lev 15)")
            if not math.isfinite(parsed) or parsed <= 0:
                return ("error", "Leverage ต้องมากกว่า 0 (1–125x)")
            updates["leverage"] = max(1.0, min(125.0, parsed))
            updated_any = True
        elif key in ("stars", "star", "min_stars", "minstars", "ดาว"):
            cleaned = raw_val.rstrip("*⭐")
            if cleaned.endswith("ดาว"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()
            try:
                parsed = int(cleaned)
            except Exception:
                return ("error", f"ระดับดาว `{raw_val}` ไม่ถูกต้อง (เลือก 2 หรือ 3 เช่น /set stars 3)")
            if parsed <= 0:
                return ("error", "ระดับดาวขั้นต่ำต้องเป็น 1–3 ดาว")
            updates["min_stars"] = max(1, min(3, parsed))
            updated_any = True
        else:
            return ("error", f"ไม่รู้จักหัวข้อ `{raw_key}` — ใช้ max, size, lev, stars หรือ default")
        i += 2

    if not updated_any:
        return ("error", "กรุณาระบุค่าที่ต้องการตั้งอย่างน้อย 1 รายการ")
    return ("update", updates)


def render_dashboard_html(st, notice_line=None):
    eq = PaperTrader.state_total_equity(st)
    init_bal = st["initial_balance"]
    net_usdt = eq - init_bal
    net_pct = (net_usdt / init_bal * 100.0) if init_bal > 0 else 0.0
    status_badge = "⏸️ พักเปิดไม้ใหม่ชั่วคราว" if st.get("paused_opening") else "🟢 เปิดรับสัญญาณอัตโนมัติ"

    eff_lev = PaperTrader.state_effective_leverage(st)
    eff_pct = PaperTrader.state_effective_position_pct(st)
    eff_max = PaperTrader.state_effective_max_open_positions(st)
    eff_stars = PaperTrader.state_effective_min_stars(st)
    custom_badge = " [ปรับเอง]" if PaperTrader.state_has_custom_settings(st) else ""

    lines = []
    if notice_line:
        lines.append(f"ℹ️ <i>{html.escape(str(notice_line))}</i>")
        lines.append("")
    lines.append("💼 <b>พอร์ตจำลองการเทรด (Paper Trading Dashboard)</b>")
    lines.append(f"• สถานะเปิดไม้ใหม่: <code>{status_badge}</code>")
    lines.append(
        f"• ตั้งค่าไม้: <code>{fmt_num_clean(eff_lev)}x | ไม้ละ {fmt_num_clean(eff_pct)}% | สูงสุด {eff_max} ไม้ | ≥ {eff_stars}⭐{custom_badge}</code>"
    )
    lines.append(f"• มูลค่าพอร์ต (Equity): <code>${eq:.2f}</code> (<code>{net_usdt:+.2f} USDT | {net_pct:+.2f}%</code>)")
    lines.append(
        f"• ทุนตั้งต้น: <code>${init_bal:.2f}</code> | เงินว่าง: <code>${st['free_balance']:.2f}</code> | Margin ถืออยู่: <code>${PaperTrader.state_locked_margin(st):.2f}</code>"
    )
    lines.append(
        f"• uPnL: <code>{PaperTrader.state_unrealized_pnl(st):+.2f} USDT</code> | Realized: <code>{st['realized_pnl']:+.2f} USDT</code> | ค่าธรรมเนียม: <code>${st['total_fees']:.2f}</code>"
    )
    lines.append(
        f"• สถิติ: <code>{st['total_closed']} ไม้</code> | Win Rate <code>{PaperTrader.state_win_rate_pct(st):.1f}%</code> (<code>{st['win_count']}W/{st['loss_count']}L</code>) | Max DD <code>{st['max_drawdown_pct']:.2f}%</code>"
    )
    lines.append("")

    open_pos = st.get("open_positions", [])
    if not open_pos:
        lines.append(f"📌 <b>ออเดอร์ที่ถืออยู่ (0/{eff_max}):</b> ยังไม่มี (รอสัญญาณ ≥ {eff_stars} ดาว)")
    else:
        lines.append(f"📌 <b>ออเดอร์ที่ถืออยู่ ({len(open_pos)}/{eff_max} ไม้):</b>")
        for idx, p in enumerate(open_pos, start=1):
            side_icon = "🟢 LONG" if p["side"] == "Long" else "🔴 SHORT"
            stage = "TP1+TP2 (ถือ 30%)" if p["tp2_hit"] else ("กันทุน BE (ถือ 60%)" if p["tp1_hit"] else "รอ TP1 (ถือ 100%)")
            upnl = PaperTrader.pos_unrealized_pnl(p)
            uroe = PaperTrader.pos_unrealized_roe_pct(p)
            lines.append(
                f"{idx}) #{p['id']} {side_icon} <b>{html.escape(p['symbol'])}</b> | <code>${fmt_price(p['entry_price'])}</code> → <code>${fmt_price(p['last_price'])}</code>"
            )
            lines.append(
                f"   uPnL: <code>{upnl:+.2f} USDT ({uroe:+.1f}%)</code> | SL: <code>${fmt_price(p['current_sl'])}</code> | {stage}"
            )

    lines.append(f"\n🕒 <i>อัปเดตล่าสุด: {datetime.now().strftime('%H:%M:%S')}</i>")

    toggle_btn = (
        {"text": "▶️ เปิดรับไม้ใหม่", "callback_data": "paper:resume"}
        if st.get("paused_opening")
        else {"text": "⏸️ พักเปิดไม้ใหม่", "callback_data": "paper:pause"}
    )
    rows = [
        [
            {"text": "🔄 รีเฟรชพอร์ต", "callback_data": "paper:refresh"},
            {"text": "📜 ประวัติ 10 รายการ", "callback_data": "paper:history"},
        ],
        [
            toggle_btn,
            {"text": "🔍 สแกนทันที", "callback_data": "paper:scan"},
        ],
    ]
    if open_pos:
        rows.append(
            [
                {"text": "📋 ปิดออเดอร์รายตัว", "callback_data": "paper:manage"},
                {"text": "❌ ปิดทุกไม้ทันที", "callback_data": "paper:close_all_prompt"},
            ]
        )
    rows.append(
        [
            {"text": "⚙️ ตั้งค่าพอร์ต", "callback_data": "paper:settings"},
            {"text": f"🔄 รีเซ็ตพอร์ต (${PAPER_INITIAL_BALANCE:.0f})", "callback_data": "paper:reset_prompt"},
        ]
    )
    return "\n".join(lines), rows


def render_settings_html(st, notice_line=None):
    eff_max = PaperTrader.state_effective_max_open_positions(st)
    eff_pct = PaperTrader.state_effective_position_pct(st)
    eff_lev = PaperTrader.state_effective_leverage(st)
    eff_stars = PaperTrader.state_effective_min_stars(st)

    eq = max(0.0, PaperTrader.state_total_equity(st))
    est_margin = eq * (eff_pct / 100.0)
    est_notional = est_margin * eff_lev
    total_alloc_pct = eff_max * eff_pct
    open_cnt = len(st.get("open_positions", []))

    source_badge = (
        "🎛️ <code>ปรับแต่งผ่าน Telegram</code> (จดจำถาวรข้ามรีสตาร์ท/รีเซ็ตพอร์ต)"
        if PaperTrader.state_has_custom_settings(st)
        else "📄 <code>ใช้ค่าเริ่มต้นจาก .env</code>"
    )
    stars_desc = "เฉพาะ 3⭐ A+ Setup" if eff_stars >= 3 else f"≥ {eff_stars}⭐ ขึ้นไป ({eff_stars}–3 ดาว)"

    lines = []
    if notice_line:
        lines.append(f"ℹ️ <i>{html.escape(str(notice_line))}</i>")
        lines.append("")
    lines.append("⚙️ <b>ตั้งค่าจำนวนไม้ & ขนาดไม้พอร์ตจำลอง</b>")
    lines.append(f"• แหล่งค่าตั้ง: {source_badge}")
    lines.append(f"• จำนวนไม้สูงสุด (Max): <code>{eff_max} ไม้</code> (กำลังถืออยู่ {open_cnt}/{eff_max})")
    lines.append(
        f"• ขนาดต่อไม้ (Size): <code>{fmt_num_clean(eff_pct)}%</code> (~<code>${est_margin:.2f} USDT</code>/ไม้ @ Equity <code>${eq:.2f}</code>)"
    )
    lines.append(
        f"• ตัวคูณ Leverage: <code>{fmt_num_clean(eff_lev)}x</code> (มูลค่าสัญญา ~<code>${est_notional:.2f} USDT</code>/ไม้)"
    )
    lines.append(f"• เกณฑ์เข้าสัญญาณ: <code>{stars_desc}</code>")
    lines.append(
        f"• โควตารวมสูงสุด (Max × Size): <code>{fmt_num_clean(total_alloc_pct)}%</code> | "
        f"ค่าเริ่มต้น .env: <code>{PAPER_MAX_OPEN_POSITIONS} ไม้ / {fmt_num_clean(PAPER_POSITION_PCT)}% / {fmt_num_clean(PAPER_LEVERAGE)}x / ≥{PAPER_MIN_STARS}⭐</code>"
    )

    if eff_lev >= 20.0:
        lines.append("")
        lines.append(
            f"⚠️ <b>คำเตือน Leverage สูง ({fmt_num_clean(eff_lev)}x):</b> ระบบวาง Stop Loss กว้างสุด ~4.8% "
            "จึงเสี่ยงถูก <b>Liquidate ล้าง Margin ไม้นั้น (-100%)</b> ก่อนถึงจุด SL หากราคาสะบัดแรง"
        )
    if total_alloc_pct > 100.0:
        lines.append("")
        lines.append(
            f"⚠️ <b>คำเตือนสัดส่วน Margin เกิน 100% ({eff_max} × {fmt_num_clean(eff_pct)}% = {fmt_num_clean(total_alloc_pct)}%):</b> "
            "เงินว่างจะถูกใช้เต็มก่อนครบทุกโควตา ไม้ถัดไปจะรอจนกว่ามีเงินว่างคืนหลังปิดไม้/ชน TP1"
        )

    lines.append("")
    lines.append("🎛️ <b>กดปุ่มด้านล่าง (แถว: Max ไม้ → ขนาด% → Leverage → ดาว):</b>")
    lines.append(
        "💡 <b>หรือพิมพ์ปรับค่าละเอียด:</b> <code>/set max 8 lev 15</code> · <code>/set size 12</code> · <code>/set stars 3</code> · <code>/set default</code>"
    )
    lines.append("<i>หมายเหตุ: ไม้ที่ถืออยู่แล้วคง Margin/Lev เดิม — ค่าใหม่มีผลตั้งแต่ไม้ถัดไป</i>")

    max_row = [
        {"text": (f"✅{m}ไม้" if eff_max == m else f"{m}ไม้"), "callback_data": f"paper:set:max:{m}"}
        for m in (3, 5, 8, 10, 15)
    ]
    size_row = [
        {"text": (f"✅{s}%" if abs(eff_pct - s) < 0.01 else f"{s}%"), "callback_data": f"paper:set:size:{s}"}
        for s in (5, 10, 15, 20, 25)
    ]
    lev_row = [
        {"text": (f"✅{lv}x" if abs(eff_lev - lv) < 0.01 else f"{lv}x"), "callback_data": f"paper:set:lev:{lv}"}
        for lv in (3, 5, 10, 15, 20)
    ]
    stars_row = [
        {"text": ("✅ 2⭐ ขึ้นไป" if eff_stars == 2 else "2⭐ ขึ้นไป"), "callback_data": "paper:set:stars:2"},
        {"text": ("✅ เฉพาะ 3⭐ A+" if eff_stars == 3 else "เฉพาะ 3⭐ A+"), "callback_data": "paper:set:stars:3"},
    ]
    footer_row = [
        {"text": "🔄 คืนค่าเริ่มต้น (.env)", "callback_data": "paper:cfg_default"},
        {"text": "⬅️ กลับหน้าพอร์ต", "callback_data": "paper:refresh"},
    ]
    return "\n".join(lines), [max_row, size_row, lev_row, stars_row, footer_row]


def render_manage_html(st):
    open_pos = st.get("open_positions", [])
    if not open_pos:
        return (
            "📋 <b>จัดการออเดอร์ที่เปิดอยู่</b>\n\nไม่มีออเดอร์ที่ถืออยู่ในขณะนี้",
            [[{"text": "⬅️ กลับหน้าพอร์ต", "callback_data": "paper:refresh"}]],
        )
    lines = ["📋 <b>จัดการออเดอร์ที่เปิดอยู่ (เลือกกดเพื่อปิดทันที)</b>", ""]
    rows = []
    for p in open_pos:
        upnl = PaperTrader.pos_unrealized_pnl(p)
        uroe = PaperTrader.pos_unrealized_roe_pct(p)
        side_icon = "🟢" if p["side"] == "Long" else "🔴"
        lines.append(
            f"• #{p['id']} {side_icon} <b>{html.escape(p['symbol'])}</b> | ราคา <code>${fmt_price(p['last_price'])}</code> | uPnL <code>{upnl:+.2f} ({uroe:+.1f}%)</code>"
        )
        rows.append([{"text": f"❌ ปิด #{p['id']} {p['symbol']} ({upnl:+.2f} USDT)", "callback_data": f"paper:close:{p['id']}"}])
    rows.append(
        [
            {"text": "⬅️ กลับหน้าพอร์ต", "callback_data": "paper:refresh"},
            {"text": "❌ ปิดทุกไม้", "callback_data": "paper:close_all_prompt"},
        ]
    )
    return "\n".join(lines), rows


def render_history_html(rows):
    lines = ["📜 <b>ประวัติจำลองการเทรด 10 รายการล่าสุด</b>", ""]
    if not rows:
        lines.append("ยังไม่มีประวัติการเปิด/ปิดออเดอร์จำลอง")
    else:
        icon_map = {
            "OPEN": "🧪",
            "TP1_PARTIAL": "💰",
            "TP2_PARTIAL": "🚀",
            "TP3_FULL_CLOSE": "🏆",
            "BREAKEVEN_SL": "✅",
            "TRAILING_SL_TP1": "✅",
            "MANUAL_CLOSE": "❌",
            "STOP_LOSS": "🛑",
        }
        for r in rows:
            ts = r["timestamp"][11:19] if len(r["timestamp"]) >= 19 else r["timestamp"]
            icon = icon_map.get(r["event"], "📌")
            lines.append(
                f"{icon} <code>{html.escape(ts)}</code> #{html.escape(str(r['trade_id']))} <b>{html.escape(r['symbol'])}</b> ({html.escape(r['side'])}) | "
                f"<code>{html.escape(r['event'])}</code> | ${fmt_price(r['exec_price'])} | <code>{r['net_pnl']:+.2f} USDT</code>"
            )
    kb = [
        [
            {"text": "🔄 รีเฟรชประวัติ", "callback_data": "paper:history"},
            {"text": "⬅️ กลับหน้าพอร์ต", "callback_data": "paper:refresh"},
        ]
    ]
    return "\n".join(lines), kb


def render_close_all_confirm_html(st):
    open_pos = st.get("open_positions", [])
    if not open_pos:
        return (
            "ℹ️ ไม่มีออเดอร์ที่เปิดอยู่ในขณะนี้",
            [[{"text": "⬅️ กลับหน้าพอร์ต", "callback_data": "paper:refresh"}]],
        )
    text = (
        f"⚠️ <b>ยืนยันปิดออเดอร์จำลองทั้งหมด ({len(open_pos)} ไม้)</b>\n\n"
        f"ระบบจะปิดทุกไม้ที่ถืออยู่ด้วยราคาตลาดปัจจุบันทันที (uPnL รวม <code>{PaperTrader.state_unrealized_pnl(st):+.2f} USDT</code>)\n"
        "ต้องการดำเนินการต่อหรือไม่?"
    )
    kb = [
        [
            {"text": "✅ ยืนยันปิดทุกไม้ทันที", "callback_data": "paper:close_all_confirm"},
            {"text": "⬅️ ยกเลิก", "callback_data": "paper:refresh"},
        ]
    ]
    return text, kb


def render_reset_confirm_html():
    text = (
        "⚠️ <b>ยืนยันการรีเซ็ตพอร์ตจำลอง</b>\n\n"
        f"การรีเซ็ตจะล้างออเดอร์ที่ถืออยู่ทั้งหมด สถิติ Win Rate และไฟล์ประวัติเทรด CSV แล้วตั้งต้นใหม่เป็น <code>${PAPER_INITIAL_BALANCE:.2f} USDT</code> "
        "<i>(ค่าตั้ง Max ไม้/Size/Lev ที่ปรับไว้จะยังคงเดิม)</i>\n"
        "ต้องการยืนยันหรือไม่?"
    )
    kb = [
        [
            {"text": "✅ ยืนยันรีเซ็ตพอร์ต", "callback_data": "paper:reset_confirm"},
            {"text": "⬅️ ยกเลิก", "callback_data": "paper:refresh"},
        ]
    ]
    return text, kb


class TelegramPaperBotController:
    def __init__(self, trader):
        self.trader = trader
        self._started = False

    @staticmethod
    def is_enabled():
        return PAPER_TRADING_ENABLED and PAPER_TELEGRAM_BOT_ENABLED and bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

    def send_dashboard_message(self, refresh_prices=True, notice=None):
        if not self.is_enabled():
            return
        if refresh_prices and self.trader.get_state_snapshot().get("open_positions"):
            try:
                pmap = fetch_all_prices()
                self.trader.update_prices(pmap)
            except Exception as e:
                print(f"⚠️ อัปเดตราคาบน Telegram Dashboard ไม่สำเร็จ: {e}")
        st = self.trader.get_state_snapshot()
        html_text, kb = render_dashboard_html(st, notice_line=notice)
        try:
            tg_send_html(html_text, kb)
        except Exception as e:
            print(f"⚠️ ส่ง Telegram Portfolio Dashboard ไม่สำเร็จ: {e}")

    def start(self):
        if self._started or not self.is_enabled():
            return
        self._started = True

        t = threading.Thread(target=self._poll_loop, daemon=True)
        t.start()

        if PAPER_TELEGRAM_SUMMARY_MINS > 0:
            tp = threading.Thread(target=self._periodic_summary_loop, daemon=True)
            tp.start()

    def _periodic_summary_loop(self):
        interval = max(60, PAPER_TELEGRAM_SUMMARY_MINS * 60)
        while True:
            time.sleep(interval)
            self.send_dashboard_message(refresh_prices=True, notice="⏰ สรุปพอร์ตจำลองตามรอบเวลา")

    def _is_authorized(self, chat_id):
        return str(chat_id) == str(TELEGRAM_CHAT_ID)

    def _poll_loop(self):
        print("🤖 เปิดใช้งาน Telegram Bot Listener สำหรับดูพอร์ตจำลองและปุ่มสั่งการเรียลไทม์")
        tg_set_commands()

        next_offset = None
        try:
            stale = tg_api_post(
                "getUpdates",
                {"offset": -1, "timeout": 0, "allowed_updates": ["message", "callback_query"]},
                timeout=10,
            ).get("result", [])
            if stale:
                next_offset = max(u.get("update_id", 0) for u in stale) + 1
        except Exception:
            pass

        while True:
            try:
                payload = {"timeout": 18, "allowed_updates": ["message", "callback_query"]}
                if next_offset is not None:
                    payload["offset"] = next_offset
                resp = tg_api_post("getUpdates", payload, timeout=25)
                for upd in resp.get("result", []):
                    uid = upd.get("update_id")
                    if uid is not None:
                        next_offset = uid + 1
                    self._handle_update(upd)
            except Exception as e:
                err_str = str(e)
                if "409" in err_str or "Conflict" in err_str:
                    print("⚠️ พบ Telegram getUpdates ซ้อน (409 Conflict) — รอ 10 วินาทีแล้วลองใหม่")
                    time.sleep(10)
                else:
                    time.sleep(5)

    def _handle_update(self, upd):
        if "callback_query" in upd:
            cb = upd["callback_query"]
            cb_id = cb.get("id", "")
            data = cb.get("data", "")
            msg = cb.get("message", {})
            chat_id = msg.get("chat", {}).get("id", 0)
            msg_id = msg.get("message_id", 0)
            if not self._is_authorized(chat_id):
                tg_answer_callback(cb_id, "⛔ เฉพาะเจ้าของบอทเท่านั้น", show_alert=True)
                return
            self._handle_callback(cb_id, data, chat_id, msg_id)
            return

        if "message" in upd:
            msg = upd["message"]
            chat_id = msg.get("chat", {}).get("id", 0)
            if not self._is_authorized(chat_id):
                return
            text = (msg.get("text") or "").strip()
            if text.startswith("/"):
                parts = text.split(None, 1)
                cmd = parts[0].split("@")[0].lower()
                args = parts[1].strip() if len(parts) > 1 else ""
                self._handle_command(cmd, args)

    def _handle_command(self, cmd, args=""):
        if cmd in ("/start", "/portfolio", "/status", "/positions"):
            self.send_dashboard_message(refresh_prices=True)
        elif cmd in ("/settings", "/config"):
            st = self.trader.get_state_snapshot()
            html_text, kb = render_settings_html(st)
            try:
                tg_send_html(html_text, kb)
            except Exception:
                pass
        elif cmd == "/set":
            if not args:
                st = self.trader.get_state_snapshot()
                html_text, kb = render_settings_html(
                    st,
                    notice_line="💡 พิมพ์ค่าที่ต้องการต่อท้าย /set เช่น /set max 8 lev 15 หรือ /set default",
                )
            else:
                mode, payload = parse_set_command_args(args)
                if mode == "reset":
                    st = self.trader.clear_runtime_settings()
                    html_text, kb = render_settings_html(st, notice_line="🔄 คืนค่าเริ่มต้นจากไฟล์ .env เรียบร้อยแล้ว")
                elif mode == "update":
                    st = self.trader.update_runtime_settings(**payload)
                    notice = (
                        f"✅ บันทึกค่าใหม่แล้ว: Max {PaperTrader.state_effective_max_open_positions(st)} ไม้ | "
                        f"ไม้ละ {fmt_num_clean(PaperTrader.state_effective_position_pct(st))}% | "
                        f"{fmt_num_clean(PaperTrader.state_effective_leverage(st))}x | "
                        f"≥ {PaperTrader.state_effective_min_stars(st)}⭐"
                    )
                    html_text, kb = render_settings_html(st, notice_line=notice)
                else:
                    st = self.trader.get_state_snapshot()
                    html_text, kb = render_settings_html(st, notice_line=f"⚠️ {payload}")
            try:
                tg_send_html(html_text, kb)
            except Exception:
                pass
        elif cmd == "/history":
            rows = self.trader.recent_csv_events(10)
            html_text, kb = render_history_html(rows)
            try:
                tg_send_html(html_text, kb)
            except Exception:
                pass
        elif cmd == "/pause":
            st = self.trader.set_paused(True)
            html_text, kb = render_dashboard_html(st, notice_line="⏸️ พักการเปิดออเดอร์ใหม่แล้ว (ออเดอร์ที่ถืออยู่ยังรัน TP/SL ต่อ)")
            try:
                tg_send_html(html_text, kb)
            except Exception:
                pass
        elif cmd == "/resume":
            st = self.trader.set_paused(False)
            html_text, kb = render_dashboard_html(st, notice_line="▶️ เปิดรับออเดอร์จำลองใหม่อีกครั้งแล้ว")
            try:
                tg_send_html(html_text, kb)
            except Exception:
                pass
        elif cmd == "/scan":
            if scan_in_progress_lock.locked():
                try:
                    tg_send_html("⏳ ขณะนี้กำลังสแกนตลาดอยู่แล้ว กรุณารอสักครู่...", None)
                except Exception:
                    pass
            else:
                try:
                    tg_send_html("🔍 กำลังเริ่มสแกนตลาด MTF ทันที...", None)
                except Exception:
                    pass
                scan_now_event.set()
        elif cmd == "/closeall":
            st = self.trader.get_state_snapshot()
            html_text, kb = render_close_all_confirm_html(st)
            try:
                tg_send_html(html_text, kb)
            except Exception:
                pass
        elif cmd == "/reset":
            html_text, kb = render_reset_confirm_html()
            try:
                tg_send_html(html_text, kb)
            except Exception:
                pass
        elif cmd == "/help":
            help_text = (
                "🤖 <b>คำสั่งควบคุมพอร์ตจำลอง (Scanner-f)</b>\n"
                "• /portfolio — ดูสรุปพอร์ตจำลอง + ปุ่มสั่งการ\n"
                "• /settings — เปิดหน้าตั้งค่า Max ไม้ / ขนาด% / Leverage / ดาว\n"
                "• /set — ปรับค่าเอง เช่น <code>/set max 8 lev 15</code> หรือ <code>/set default</code>\n"
                "• /history — ดูประวัติการเทรด 10 รายการล่าสุด\n"
                "• /scan — สแกนตลาด MTF ทันที 1 รอบ\n"
                "• /pause — พักการเปิดออเดอร์ใหม่ชั่วคราว\n"
                "• /resume — เปิดรับออเดอร์ใหม่อีกครั้ง\n"
                "• /closeall — ปิดออเดอร์จำลองทั้งหมดทันที (มีหน้ายืนยัน)\n"
                "• /reset — รีเซ็ตพอร์ตจำลองกลับเป็นทุนเริ่มต้น (มีหน้ายืนยัน)"
            )
            kb = [
                [
                    {"text": "💼 เปิดพอร์ตจำลอง", "callback_data": "paper:panel"},
                    {"text": "⚙️ ตั้งค่าพอร์ต", "callback_data": "paper:settings"},
                ]
            ]
            try:
                tg_send_html(help_text, kb)
            except Exception:
                pass

    def _handle_callback(self, cb_id, data, chat_id, msg_id):
        try:
            if data == "paper:panel":
                tg_answer_callback(cb_id, "💼 กำลังเปิดหน้าพอร์ตล่าสุด...")
                self.send_dashboard_message(refresh_prices=True)
            elif data == "paper:refresh":
                tg_answer_callback(cb_id, "🔄 อัปเดตราคาและพอร์ตล่าสุดแล้ว")
                try:
                    pmap = fetch_all_prices()
                    self.trader.update_prices(pmap)
                except Exception:
                    pass
                st = self.trader.get_state_snapshot()
                html_text, kb = render_dashboard_html(st)
                tg_edit_html(chat_id, msg_id, html_text, kb)
            elif data == "paper:settings":
                tg_answer_callback(cb_id, "⚙️ เปิดหน้าตั้งค่าพอร์ตจำลอง")
                st = self.trader.get_state_snapshot()
                html_text, kb = render_settings_html(st)
                tg_edit_html(chat_id, msg_id, html_text, kb)
            elif data == "paper:cfg_default":
                st = self.trader.clear_runtime_settings()
                tg_answer_callback(cb_id, "🔄 คืนค่าเริ่มต้นจาก .env แล้ว")
                html_text, kb = render_settings_html(st, notice_line="🔄 คืนค่าเริ่มต้นจากไฟล์ .env เรียบร้อยแล้ว")
                tg_edit_html(chat_id, msg_id, html_text, kb)
            elif data.startswith("paper:set:"):
                parts = data.split(":", 3)
                field = parts[2] if len(parts) > 2 else ""
                raw_val = parts[3] if len(parts) > 3 else ""
                toast = None
                notice = None
                st = None
                try:
                    if field == "max":
                        st = self.trader.update_runtime_settings(max_open_positions=int(raw_val))
                        eff = PaperTrader.state_effective_max_open_positions(st)
                        toast = f"✅ ตั้งจำนวนไม้สูงสุดเป็น {eff} ไม้แล้ว"
                        notice = f"✅ ปรับจำนวนไม้สูงสุดเป็น {eff} ไม้แล้ว (มีผลทันทีตั้งแต่ไม้ถัดไป)"
                    elif field == "size":
                        st = self.trader.update_runtime_settings(position_pct=float(raw_val))
                        eff = PaperTrader.state_effective_position_pct(st)
                        toast = f"✅ ตั้งขนาดไม้เป็น {fmt_num_clean(eff)}% แล้ว"
                        notice = f"✅ ปรับขนาดต่อไม้เป็น {fmt_num_clean(eff)}% แล้ว (มีผลทันทีตั้งแต่ไม้ถัดไป)"
                    elif field == "lev":
                        st = self.trader.update_runtime_settings(leverage=float(raw_val))
                        eff = PaperTrader.state_effective_leverage(st)
                        toast = f"✅ ตั้ง Leverage เป็น {fmt_num_clean(eff)}x แล้ว"
                        notice = f"✅ ปรับ Leverage เป็น {fmt_num_clean(eff)}x แล้ว (มีผลทันทีตั้งแต่ไม้ถัดไป)"
                    elif field == "stars":
                        st = self.trader.update_runtime_settings(min_stars=int(raw_val))
                        eff = PaperTrader.state_effective_min_stars(st)
                        lbl = "เฉพาะ 3⭐ A+" if eff >= 3 else f"≥ {eff}⭐ ขึ้นไป"
                        toast = f"✅ ตั้งเกณฑ์เข้าสัญญาณเป็น {lbl} แล้ว"
                        notice = f"✅ ปรับเกณฑ์รับสัญญาณเป็น {lbl} แล้ว"
                except Exception:
                    st = None
                if st is not None:
                    tg_answer_callback(cb_id, toast)
                    html_text, kb = render_settings_html(st, notice_line=notice)
                    tg_edit_html(chat_id, msg_id, html_text, kb)
                else:
                    tg_answer_callback(cb_id, "⚠️ ค่าตั้งค่าไม่ถูกต้อง", show_alert=True)
            elif data == "paper:history":
                tg_answer_callback(cb_id, "📜 โหลดประวัติเทรดล่าสุดแล้ว")
                rows = self.trader.recent_csv_events(10)
                html_text, kb = render_history_html(rows)
                tg_edit_html(chat_id, msg_id, html_text, kb)
            elif data == "paper:history_new":
                tg_answer_callback(cb_id, "📜 เปิดประวัติเทรดล่าสุดแล้ว")
                rows = self.trader.recent_csv_events(10)
                html_text, kb = render_history_html(rows)
                tg_send_html(html_text, kb)
            elif data == "paper:pause":
                tg_answer_callback(cb_id, "⏸️ พักการเปิดไม้ใหม่แล้ว")
                st = self.trader.set_paused(True)
                html_text, kb = render_dashboard_html(st, notice_line="⏸️ พักการเปิดออเดอร์ใหม่ชั่วคราว (ออเดอร์เดิมยังดูแล TP/SL ตามปกติ)")
                tg_edit_html(chat_id, msg_id, html_text, kb)
            elif data == "paper:resume":
                tg_answer_callback(cb_id, "▶️ เปิดรับออเดอร์ใหม่แล้ว")
                st = self.trader.set_paused(False)
                html_text, kb = render_dashboard_html(st, notice_line="▶️ เปิดรับออเดอร์จำลองจากสัญญาณตามเกณฑ์ที่ตั้งไว้แล้ว")
                tg_edit_html(chat_id, msg_id, html_text, kb)
            elif data == "paper:scan":
                if scan_in_progress_lock.locked():
                    tg_answer_callback(cb_id, "⏳ กำลังสแกนตลาดอยู่แล้ว กรุณารอสักครู่...")
                else:
                    tg_answer_callback(cb_id, "🔍 กำลังเริ่มสแกนตลาดรอบใหม่ทันที!")
                    scan_now_event.set()
                try:
                    if self.trader.get_state_snapshot().get("open_positions"):
                        pmap = fetch_all_prices()
                        self.trader.update_prices(pmap)
                except Exception:
                    pass
                st = self.trader.get_state_snapshot()
                html_text, kb = render_dashboard_html(st, notice_line="🔍 รับคำสั่งสแกนตลาด MTF ทันทีแล้ว")
                tg_edit_html(chat_id, msg_id, html_text, kb)
            elif data == "paper:manage":
                tg_answer_callback(cb_id, "📋 เลือกออเดอร์ที่ต้องการปิด")
                try:
                    pmap = fetch_all_prices()
                    self.trader.update_prices(pmap)
                except Exception:
                    pass
                st = self.trader.get_state_snapshot()
                html_text, kb = render_manage_html(st)
                tg_edit_html(chat_id, msg_id, html_text, kb)
            elif data == "paper:close_all_prompt":
                tg_answer_callback(cb_id)
                st = self.trader.get_state_snapshot()
                html_text, kb = render_close_all_confirm_html(st)
                tg_edit_html(chat_id, msg_id, html_text, kb)
            elif data == "paper:close_all_confirm":
                pmap = None
                try:
                    pmap = fetch_all_prices()
                except Exception:
                    pass
                cnt, sum_pnl = self.trader.close_all_positions(market_prices=pmap)
                tg_answer_callback(cb_id, f"❌ ปิดทั้งหมด {cnt} ไม้ ({sum_pnl:+.2f} USDT)")
                st = self.trader.get_state_snapshot()
                html_text, kb = render_dashboard_html(st, notice_line=f"❌ ปิดออเดอร์ด้วยตนเอง {cnt} ไม้ (PnL รวม {sum_pnl:+.2f} USDT)")
                tg_edit_html(chat_id, msg_id, html_text, kb)
            elif data == "paper:reset_prompt":
                tg_answer_callback(cb_id)
                html_text, kb = render_reset_confirm_html()
                tg_edit_html(chat_id, msg_id, html_text, kb)
            elif data == "paper:reset_confirm":
                st = self.trader.reset_portfolio()
                tg_answer_callback(cb_id, "🔄 รีเซ็ตพอร์ตจำลองกลับเป็นทุนเริ่มต้นเรียบร้อย!")
                html_text, kb = render_dashboard_html(st, notice_line="🔄 รีเซ็ตพอร์ตและล้างประวัติเรียบร้อยแล้ว (คงค่าตั้งขนาดไม้/Lev ที่ปรับไว้)")
                tg_edit_html(chat_id, msg_id, html_text, kb)
            elif data.startswith("paper:close:"):
                try:
                    tid = int(data.split(":", 2)[2])
                except Exception:
                    tg_answer_callback(cb_id)
                    return
                pmap = None
                try:
                    pmap = fetch_all_prices()
                except Exception:
                    pass
                closed = self.trader.close_position_by_id(tid, market_prices=pmap)
                if closed:
                    sym, pnl = closed
                    tg_answer_callback(cb_id, f"✅ ปิด #{tid} {sym} ({pnl:+.2f} USDT)")
                    notice = f"✅ ปิดไม้ #{tid} {sym} แล้ว ({pnl:+.2f} USDT)"
                else:
                    tg_answer_callback(cb_id, "⚠️ ไม่พบไม้นี้ (อาจปิดไปแล้ว)")
                    notice = None
                st = self.trader.get_state_snapshot()
                html_text, kb = render_dashboard_html(st, notice_line=notice)
                tg_edit_html(chat_id, msg_id, html_text, kb)
            else:
                tg_answer_callback(cb_id)
        except Exception as e:
            print(f"⚠️ จัดการปุ่ม Telegram ไม่สำเร็จ: {e}")


telegram_bot = TelegramPaperBotController(paper_trader)


def dispatch_mtf_alert(
    symbol,
    category_key,
    title_th,
    direction,
    price,
    chg24,
    vol24,
    trigger_tf,
    summary,
    state_5m,
    state_15m,
    state_1h,
    grade_str,
    aligned_1h,
    reasons,
    extra_details=None,
    force=False,
):
    now = time.time()
    cd_key = f"{symbol}:{category_key}"
    if not force:
        last_ts = cooldown_cache.get(cd_key, 0)
        if now - last_ts < ALERT_COOLDOWN_SECS:
            return None
        cooldown_cache[cd_key] = now

    plan = build_mtf_trade_plan(direction, price, state_5m, state_15m, state_1h, aligned_1h)
    ts_str = datetime.now().strftime("%H:%M:%S")
    reason_str = " + ".join(reasons) if reasons else "สัญญาณเทคนิคเข้าเงื่อนไข"

    print(f"\n🔔 [{ts_str}] {title_th} | {grade_str}")
    print(f"   🪙 #{symbol} (Trigger: {trigger_tf}) | ราคา: ${fmt_price(price)} | 24h: {chg24:+.2f}% | Vol 24h: ${fmt_usd(vol24)}")
    print(f"   📌 สัญญาณหลัก : {summary}")
    print(f"   ✨ ความสอดคล้อง: {reason_str}")
    if state_5m and state_15m and state_1h:
        print(f"   📊 สถานะ 3 TF :")
        print(f"      • [5m]  : {state_5m['label']} | Vol {state_5m['vol_ratio']:.1f}x")
        print(f"      • [15m] : {state_15m['label']}")
        print(f"      • [1h]  : {state_1h['label']}")
    if plan:
        print(f"   🧭 แผนเทรด    : {plan['side_label']}")
        print(f"   🎯 จุดเข้า (Entry) : {plan['entry_str']}")
        print(f"   💰 เป้าทำกำไร TP1  : {plan['tp1_str']}")
        print(f"   💰 เป้าทำกำไร TP2  : {plan['tp2_str']}")
        print(f"   💰 เป้าทำกำไร TP3  : {plan['tp3_str']}")
        print(f"   🛡️ ตัดขาดทุน (SL)  : {plan['sl_str']}")

    futures_url = f"https://www.binance.com/en/futures/{symbol}"

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        lines = [
            f"<b>{html.escape(title_th)}</b>",
            f"🏆 ระดับความแม่นยำ: <b>{html.escape(grade_str)}</b>",
            f"🪙 เหรียญ: <b>#{html.escape(symbol)}</b> (ราคา: <code>${fmt_price(price)}</code>)",
            f"📊 24h: <code>{chg24:+.2f}%</code> | 💧 Vol 24h: <code>${fmt_usd(vol24)}</code>",
            f"📌 สัญญาณ ({html.escape(trigger_tf)}): <b>{html.escape(summary)}</b>",
            f"✨ เหตุผลหนุน: <code>{html.escape(reason_str)}</code>",
        ]
        if state_5m and state_15m and state_1h:
            lines.append("")
            lines.append("🔎 <b>ภาพรวม Multi-Timeframe (5m + 15m + 1h):</b>")
            lines.append(f"• <b>TF 5m</b> : <code>{html.escape(state_5m['label'])}</code>")
            lines.append(f"• <b>TF 15m</b>: <code>{html.escape(state_15m['label'])}</code>")
            lines.append(f"• <b>TF 1h</b> : <code>{html.escape(state_1h['label'])}</code>")

        if extra_details:
            for k, v in extra_details:
                lines.append(f"• {html.escape(str(k))}: <code>{html.escape(str(v))}</code>")

        if plan:
            lines.append("")
            lines.append(f"🧭 <b>{html.escape(plan['side_label'])}</b>")
            lines.append(f"🎯 จุดเข้า (Entry): <code>{html.escape(plan['entry_str'])}</code>")
            lines.append(f"💰 TP1: <code>{html.escape(plan['tp1_str'])}</code>")
            lines.append(f"💰 TP2: <code>{html.escape(plan['tp2_str'])}</code>")
            lines.append(f"💰 TP3: <code>{html.escape(plan['tp3_str'])}</code>")
            lines.append(f"🛡️ ตัดขาดทุน (SL): <code>{html.escape(plan['sl_str'])}</code>")

        lines.append(f'\n🔗 <a href="{futures_url}">เปิดกราฟ Binance Futures ({html.escape(symbol)})</a>')
        try:
            http_post_json(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": "\n".join(lines),
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )
        except Exception as e:
            print(f"   ⚠️ ส่ง Telegram ไม่สำเร็จ: {e}")

    if DISCORD_WEBHOOK_URL:
        color = 0x00E676 if direction == "BULL" else (0xFF5252 if direction == "BEAR" else 0xFFB300)
        fields = [
            {"name": "🏆 ความแม่นยำ", "value": f"**{grade_str}**", "inline": False},
            {"name": "🪙 เหรียญ", "value": f"**{symbol}** (`${fmt_price(price)}`)", "inline": True},
            {"name": "📊 24h / Vol", "value": f"`{chg24:+.2f}%` / `${fmt_usd(vol24)}`", "inline": True},
            {"name": f"📌 สัญญาณ ({trigger_tf})", "value": f"**{summary}**\n({reason_str})", "inline": False},
        ]
        if state_5m and state_15m and state_1h:
            mtf_text = (
                f"• **5m**: `{state_5m['label']}`\n"
                f"• **15m**: `{state_15m['label']}`\n"
                f"• **1h**: `{state_1h['label']}`"
            )
            fields.append({"name": "🔎 วิเคราะห์ 3 Timeframe", "value": mtf_text, "inline": False})
        if plan:
            fields.extend(
                [
                    {"name": "🧭 แผนเทรด", "value": f"**{plan['side_label']}**", "inline": False},
                    {"name": "🎯 จุดเข้า (Entry)", "value": f"`{plan['entry_str']}`", "inline": True},
                    {"name": "🛡️ ตัดขาดทุน (SL)", "value": f"`{plan['sl_str']}`", "inline": True},
                    {
                        "name": "💰 เป้าทำกำไร (TP1 / TP2 / TP3)",
                        "value": f"• TP1: `{plan['tp1_str']}`\n• TP2: `{plan['tp2_str']}`\n• TP3: `{plan['tp3_str']}`",
                        "inline": False,
                    },
                ]
            )
        try:
            http_post_json(
                DISCORD_WEBHOOK_URL,
                {
                    "username": "Scanner-f MTF",
                    "embeds": [
                        {
                            "title": f"{symbol} — {title_th}",
                            "url": futures_url,
                            "color": color,
                            "fields": fields,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                    ],
                },
            )
        except Exception as e:
            print(f"   ⚠️ ส่ง Discord ไม่สำเร็จ: {e}")

    return plan


def detect_triggers_on_tf(candles, tf_name):
    """
    ตรวจหาสัญญาณตั้งต้นบน Timeframe 5m หรือ 15m
    คืนค่ารายการของ (category_key, title_th, direction, is_reversal, summary)
    """
    triggers = []
    if not candles or len(candles) < VOL_LOOKBACK + 2:
        return triggers

    latest = candles[-1]
    open_p = latest["open"]
    close_p = latest["close"]
    chg_pct = ((close_p - open_p) / open_p * 100.0) if open_p > 0 else 0.0

    prev_vols = [c["quote_vol"] for c in candles[-1 - VOL_LOOKBACK : -1]]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0.0
    vol_ratio = (latest["quote_vol"] / avg_vol) if avg_vol > 0 else 1.0

    # 1. Price Pump / Dump
    if chg_pct >= PRICE_CHANGE_PCT:
        triggers.append(
            (
                "PUMP",
                "🚀 ราคาพุ่งแรง (Price Pump)",
                "BULL",
                False,
                f"แท่ง {tf_name} พุ่ง +{chg_pct:.2f}% (Vol {vol_ratio:.1f}x)",
            )
        )
    elif chg_pct <= -PRICE_CHANGE_PCT:
        triggers.append(
            (
                "DUMP",
                "🩸 ราคาทุบแรง (Price Dump)",
                "BEAR",
                False,
                f"แท่ง {tf_name} ทุบ {chg_pct:.2f}% (Vol {vol_ratio:.1f}x)",
            )
        )

    # 2. Volume Spike
    if vol_ratio >= VOL_SPIKE_MULT and abs(chg_pct) >= 0.6:
        triggers.append(
            (
                "VOL_SPIKE",
                "📊 วอลุ่มพุ่งผิดปกติ (Volume Spike)",
                "BULL" if chg_pct >= 0 else "BEAR",
                False,
                f"วอลุ่ม {tf_name} พุ่ง {vol_ratio:.2f} เท่าของค่าเฉลี่ย ({chg_pct:+.2f}%)",
            )
        )

    closed_candles = candles[:-1] if not latest["closed"] else candles
    closes = [c["close"] for c in closed_candles]
    if len(closes) < max(EMA_SLOW + 2, BB_PERIOD, RSI_PERIOD + 1):
        return triggers

    eval_price = closes[-1]

    # 3. RSI Overbought / Oversold
    rsi = calc_rsi(closes, RSI_PERIOD)
    if rsi is not None:
        if rsi >= RSI_OB:
            triggers.append(
                (
                    "RSI_OB",
                    "🔥 RSI เข้าเขตซื้อมากเกินไป (Overbought)",
                    "BEAR",
                    True,
                    f"RSI({RSI_PERIOD}) แท่ง {tf_name} = {rsi:.2f} (>= {RSI_OB:.0f})",
                )
            )
        elif rsi <= RSI_OS:
            triggers.append(
                (
                    "RSI_OS",
                    "🧊 RSI เข้าเขตขายมากเกินไป (Oversold)",
                    "BULL",
                    True,
                    f"RSI({RSI_PERIOD}) แท่ง {tf_name} = {rsi:.2f} (<= {RSI_OS:.0f})",
                )
            )

    # 4. EMA 9/21 Crossover
    fast_emas = calc_ema_series(closes, EMA_FAST)
    slow_emas = calc_ema_series(closes, EMA_SLOW)
    if fast_emas and slow_emas:
        if fast_emas[-2] <= slow_emas[-2] and fast_emas[-1] > slow_emas[-1]:
            triggers.append(
                (
                    "EMA_BULL",
                    "📈 EMA Golden Cross (ตัดขึ้น)",
                    "BULL",
                    False,
                    f"EMA({EMA_FAST}) ตัดขึ้นเหนือ EMA({EMA_SLOW}) ในแท่ง {tf_name}",
                )
            )
        elif fast_emas[-2] >= slow_emas[-2] and fast_emas[-1] < slow_emas[-1]:
            triggers.append(
                (
                    "EMA_BEAR",
                    "📉 EMA Death Cross (ตัดลง)",
                    "BEAR",
                    False,
                    f"EMA({EMA_FAST}) ตัดลงต่ำกว่า EMA({EMA_SLOW}) ในแท่ง {tf_name}",
                )
            )

    # 5. Bollinger Bands Breakout + Volume
    bb = calc_bollinger(closes, BB_PERIOD, BB_STD)
    if bb is not None and vol_ratio >= BB_MIN_VOL_RATIO:
        upper, _, lower = bb
        if eval_price > upper:
            triggers.append(
                (
                    "BB_UPPER",
                    "⚡ ทะลุกรอบบน Bollinger Band + Volume",
                    "BULL",
                    False,
                    f"ราคาปิดทะลุ Upper BB ({tf_name}) พร้อมวอลุ่ม {vol_ratio:.1f}x",
                )
            )
        elif eval_price < lower:
            triggers.append(
                (
                    "BB_LOWER",
                    "⚠️ หลุดกรอบล่าง Bollinger Band + Volume",
                    "BEAR",
                    False,
                    f"ราคาปิดหลุด Lower BB ({tf_name}) พร้อมวอลุ่ม {vol_ratio:.1f}x",
                )
            )

    return triggers


def scan_symbol_mtf(symbol):
    """
    สแกนเหรียญแบบ Two-Stage MTF:
    1) ดึง 5m และ 15m (ใช้ Cache สำหรับ 15m ช่วง 120 วินาที) เพื่อตรวจหาสัญญาณตั้งต้น
    2) หากมีสัญญาณตั้งต้น จึงดึง/เช็ค 1h (Cache 300 วินาที) เพื่อประเมิน Confluence ครบ 3 TF (5m + 15m + 1h)
    """
    candles_5m = fetch_klines(symbol, "5m", CANDLE_LIMIT)
    triggers_5m = detect_triggers_on_tf(candles_5m, "5m")

    candles_15m = get_cached_klines(symbol, "15m", ttl_secs=120)
    triggers_15m = detect_triggers_on_tf(candles_15m, "15m")

    if not triggers_5m and not triggers_15m:
        return symbol, []

    candles_1h = get_cached_klines(symbol, "1h", ttl_secs=300)

    state_5m = analyze_tf_state(candles_5m)
    state_15m = analyze_tf_state(candles_15m)
    state_1h = analyze_tf_state(candles_1h)
    if not state_5m or not state_15m or not state_1h:
        return symbol, []

    all_raw = [("5m", t) for t in triggers_5m] + [("15m", t) for t in triggers_15m]
    seen_categories = set()
    confirmed_results = []

    for tf_name, (cat_key, title_th, direction, is_reversal, summary) in all_raw:
        if cat_key in seen_categories:
            continue
        stars, grade_str, aligned_1h, reasons = evaluate_mtf_confluence(
            direction, is_reversal, state_5m, state_15m, state_1h
        )
        if stars >= MIN_STARS:
            seen_categories.add(cat_key)
            confirmed_results.append(
                {
                    "tf": tf_name,
                    "cat_key": cat_key,
                    "title_th": title_th,
                    "direction": direction,
                    "summary": summary,
                    "stars": stars,
                    "grade_str": grade_str,
                    "aligned_1h": aligned_1h,
                    "reasons": reasons,
                    "state_5m": state_5m,
                    "state_15m": state_15m,
                    "state_1h": state_1h,
                }
            )

    return symbol, confirmed_results


def scan_once():
    print(f"\n⏳ [{datetime.now().strftime('%H:%M:%S')}] กำลังสแกนตลาด Binance Futures แบบ Multi-TF (5m + 15m + 1h)...")
    ex_info = http_get_json(f"{FAPI_URL}/fapi/v1/exchangeInfo")
    valid_perps = {
        s["symbol"]
        for s in ex_info.get("symbols", [])
        if s.get("contractType") == "PERPETUAL"
        and s.get("status") == "TRADING"
        and s.get("quoteAsset") == "USDT"
    }

    tickers = http_get_json(f"{FAPI_URL}/fapi/v1/ticker/24hr")
    all_price_map = {}
    qualified = []
    ticker_map = {}
    for t in tickers:
        sym = t["symbol"]
        last_p = float(t.get("lastPrice", 0.0))
        if last_p > 0:
            all_price_map[sym] = last_p
        if sym not in valid_perps:
            continue
        qvol = float(t.get("quoteVolume", 0.0))
        if qvol >= MIN_QUOTE_VOL_24H:
            qualified.append((qvol, sym))
            ticker_map[sym] = {
                "price": last_p,
                "chg24": float(t.get("priceChangePercent", 0.0)),
                "vol24": qvol,
            }

    # อัปเดตราคาปัจจุบันของออเดอร์จำลองที่เปิดค้างอยู่ทั้งหมดก่อนสแกนรอบใหม่
    paper_trader.update_prices(all_price_map)

    qualified.sort(reverse=True)
    symbols = [sym for _, sym in qualified[:MAX_SYMBOLS]]
    print(f"✅ คัดกรองเหรียญ Volume >= ${fmt_usd(MIN_QUOTE_VOL_24H)} ได้ {len(symbols)} เหรียญ (แจ้งเตือนเฉพาะ >= {MIN_STARS} ดาว ⭐⭐)")

    signal_total = 0

    # 1. สแกน MTF (5m + 15m + 1h) แบบขนาน
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(scan_symbol_mtf, sym) for sym in symbols]
        for fut in concurrent.futures.as_completed(futures):
            try:
                sym, results = fut.result()
            except Exception:
                continue
            if not results:
                continue

            meta = ticker_map[sym]
            # เรียงสัญญาณที่ได้ดาวสูงสุดขึ้นก่อน
            results.sort(key=lambda x: x["stars"], reverse=True)
            for item in results:
                signal_total += 1
                latest_price = item["state_5m"]["candles"][-1]["close"]
                plan = dispatch_mtf_alert(
                    sym,
                    item["cat_key"],
                    item["title_th"],
                    item["direction"],
                    latest_price,
                    meta["chg24"],
                    meta["vol24"],
                    item["tf"],
                    item["summary"],
                    item["state_5m"],
                    item["state_15m"],
                    item["state_1h"],
                    item["grade_str"],
                    item["aligned_1h"],
                    item["reasons"],
                )
                if plan:
                    paper_trader.try_open_position(
                        sym,
                        item["direction"],
                        item["cat_key"],
                        f"{item['tf']}+15m+1h",
                        item["stars"],
                        plan,
                    )

    # 2. สแกน Funding Rate ผิดปกติ พร้อมดึง 5m+15m+1h มาวางแผนเทรด
    try:
        prem_list = http_get_json(f"{FAPI_URL}/fapi/v1/premiumIndex")
        active_set = set(symbols)
        for item in prem_list:
            sym = item.get("symbol")
            if sym not in active_set:
                continue
            fr_pct = float(item.get("lastFundingRate", 0.0)) * 100.0
            if abs(fr_pct) >= FUNDING_THRESHOLD_PCT:
                meta = ticker_map[sym]
                direction = "BULL" if fr_pct < 0 else "BEAR"
                note = "Short จ่าย Long (ระวัง Short Squeeze)" if fr_pct < 0 else "Long จ่าย Short (Long หนาแน่น)"
                c5 = fetch_klines(sym, "5m", CANDLE_LIMIT)
                c15 = get_cached_klines(sym, "15m", 120)
                c1h = get_cached_klines(sym, "1h", 300)
                s5, s15, s1h = analyze_tf_state(c5), analyze_tf_state(c15), analyze_tf_state(c1h)
                if s5 and s15 and s1h:
                    stars, grade_str, aligned_1h, reasons = evaluate_mtf_confluence(direction, True, s5, s15, s1h)
                    reasons.insert(0, f"Funding Rate สุดขั้ว ({fr_pct:+.4f}%)")
                    signal_total += 1
                    mark_price = float(item.get("markPrice", meta["price"]))
                    plan = dispatch_mtf_alert(
                        sym,
                        "FUNDING",
                        "💰 Funding Rate สูง/ต่ำผิดปกติ",
                        direction,
                        mark_price,
                        meta["chg24"],
                        meta["vol24"],
                        "Funding+MTF",
                        f"Funding Rate = {fr_pct:+.4f}% ({note})",
                        s5,
                        s15,
                        s1h,
                        grade_str,
                        aligned_1h,
                        reasons,
                    )
                    if plan:
                        paper_trader.try_open_position(
                            sym,
                            direction,
                            "FUNDING",
                            "Funding+MTF",
                            stars,
                            plan,
                        )
    except Exception as e:
        print(f"⚠️ ดึงข้อมูล Funding Rate ไม่สำเร็จ: {e}")

    print(f"🏁 จบรอบสแกน MTF: พบสัญญาณคุณภาพ (⭐⭐ ขึ้นไป) จำนวน {signal_total} รายการ")
    if PAPER_TRADING_ENABLED:
        paper_trader.print_portfolio_summary()


def run_paper_self_test():
    """ทดสอบระบบจำลองการเทรดในไฟล์ชั่วคราว (/tmp) เพื่อให้มั่นใจว่าคำนวณ TP1/TP2/TP3/SL/BE ถูกต้อง"""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        pt = PaperTrader(
            state_file=os.path.join(tmpdir, "state.json"),
            csv_file=os.path.join(tmpdir, "trades.csv"),
        )
        # 1) Long: TP1 (40%+BE) -> TP2 (30%+Trail TP1) -> TP3 (30%)
        long_plan = {
            "entry_price": 100.0,
            "sl": 98.0,
            "tp1": 102.0,
            "tp2": 104.0,
            "tp3": 106.0,
            "risk_pct": 2.0,
        }
        tid1 = pt.try_open_position("BTCUSDT", "BULL", "RSI_OS", "5m+15m+1h", 3, long_plan, notify=False)
        assert tid1 == 1
        pt.update_prices({"BTCUSDT": 102.0}, notify=False)
        assert pt.state["open_positions"][0]["tp1_hit"] is True
        assert pt.state["open_positions"][0]["current_sl"] > 100.0
        pt.update_prices({"BTCUSDT": 104.0}, notify=False)
        assert pt.state["open_positions"][0]["tp2_hit"] is True
        assert abs(pt.state["open_positions"][0]["current_sl"] - 102.0) < 1e-6
        pt.update_prices({"BTCUSDT": 106.0}, notify=False)
        assert len(pt.state["open_positions"]) == 0
        assert pt.state["win_count"] == 1
        assert pt.state["free_balance"] > 1030.0

        # 2) Short: TP1 -> Price reverses past Breakeven SL (net trade remains profitable)
        short_plan = {
            "entry_price": 100.0,
            "sl": 102.0,
            "tp1": 98.0,
            "tp2": 96.0,
            "tp3": 94.0,
            "risk_pct": 2.0,
        }
        tid2 = pt.try_open_position("ETHUSDT", "BEAR", "RSI_OB", "5m+15m+1h", 2, short_plan, notify=False)
        assert tid2 == 2
        pt.update_prices({"ETHUSDT": 98.0}, notify=False)
        assert pt.state["open_positions"][0]["tp1_hit"] is True
        pt.update_prices({"ETHUSDT": 99.95}, notify=False)
        assert len(pt.state["open_positions"]) == 0
        assert pt.state["win_count"] == 2

        # 3) Full Stop Loss before TP1 + Accounting invariant check
        sol_plan = {
            "entry_price": 100.0,
            "sl": 98.0,
            "tp1": 102.0,
            "tp2": 104.0,
            "tp3": 106.0,
            "risk_pct": 2.0,
        }
        tid3 = pt.try_open_position("SOLUSDT", "BULL", "EMA_BULL", "5m+15m+1h", 2, sol_plan, notify=False)
        assert tid3 == 3
        pt.update_prices({"SOLUSDT": 98.0}, notify=False)
        assert pt.state["total_closed"] == 3
        assert pt.state["loss_count"] == 1
        expected_eq = pt.state["initial_balance"] + pt.state["realized_pnl"] + pt.unrealized_pnl()
        assert abs(pt.total_equity() - expected_eq) < 1e-6
        assert os.path.exists(os.path.join(tmpdir, "trades.csv"))

        # 4) Pause/Resume opening + Manual Close + CSV history & HTML Dashboard rendering
        st_paused = pt.set_paused(True)
        assert st_paused["paused_opening"] is True
        assert pt.try_open_position("XRPUSDT", "BULL", "RSI_OS", "5m+15m+1h", 3, sol_plan, notify=False) is None

        st_resumed = pt.set_paused(False)
        assert st_resumed["paused_opening"] is False
        tid4 = pt.try_open_position("ADA&TEST<1>", "BULL", "RSI_OS", "5m+15m+1h", 3, sol_plan, notify=False)
        assert tid4 == 4

        dash_html, dash_kb = render_dashboard_html(pt.get_state_snapshot(), notice_line="ทดสอบ <แจ้งเตือน> & ปุ่ม")
        assert "&lt;แจ้งเตือน&gt;" in dash_html
        assert "ADA&amp;TEST&lt;1&gt;" in dash_html
        assert len(dash_kb) == 4  # Open position active -> Manage & Close All row appears

        manage_html, manage_kb = render_manage_html(pt.get_state_snapshot())
        assert "ADA&amp;TEST&lt;1&gt;" in manage_html
        assert any(btn["callback_data"] == "paper:close:4" for row in manage_kb for btn in row)

        closed = pt.close_position_by_id(4, market_prices={"ADA&TEST<1>": 103.0}, notify=False)
        assert closed is not None and closed[1] > 0
        assert pt.get_state_snapshot()["total_closed"] == 4
        expected_eq_after = pt.state["initial_balance"] + pt.state["realized_pnl"] + pt.unrealized_pnl()
        assert abs(pt.total_equity() - expected_eq_after) < 1e-6

        hist_rows = pt.recent_csv_events(10)
        assert len(hist_rows) >= 2
        assert hist_rows[0]["event"] == "MANUAL_CLOSE"
        hist_html, hist_kb = render_history_html(hist_rows)
        assert "MANUAL_CLOSE" in hist_html
        assert len(hist_kb) == 1

        # 5) Runtime settings overrides, persistence, reset preservation, /set parser & warnings
        st_cfg = pt.update_runtime_settings(max_open_positions=1, position_pct=20.0, leverage=20.0, min_stars=3)
        assert PaperTrader.state_has_custom_settings(st_cfg) is True
        assert PaperTrader.state_effective_max_open_positions(st_cfg) == 1
        assert abs(PaperTrader.state_effective_position_pct(st_cfg) - 20.0) < 1e-6
        assert abs(PaperTrader.state_effective_leverage(st_cfg) - 20.0) < 1e-6
        assert PaperTrader.state_effective_min_stars(st_cfg) == 3

        # 2-star signal blocked when min_stars=3
        assert pt.try_open_position("DOGEUSDT", "BULL", "RSI_OS", "5m+15m+1h", 2, sol_plan, notify=False) is None
        # 3-star opens with 20% equity (~20% margin) & 20x leverage
        tid5 = pt.try_open_position("AVAXUSDT", "BULL", "RSI_OS", "5m+15m+1h", 3, sol_plan, notify=False)
        assert tid5 == 5
        open_now = pt.get_state_snapshot()["open_positions"]
        assert len(open_now) == 1
        assert abs(open_now[0]["leverage"] - 20.0) < 1e-6
        # Max=1 blocks second symbol
        assert pt.try_open_position("LINKUSDT", "BULL", "RSI_OS", "5m+15m+1h", 3, sol_plan, notify=False) is None

        # High risk warnings check in Settings HTML (Lev 20x & max=8*size=20% = 160%)
        st_warn = pt.update_runtime_settings(max_open_positions=8)
        set_html, set_kb = render_settings_html(st_warn, notice_line="บันทึกทดสอบ")
        assert "คำเตือน Leverage สูง (20x)" in set_html
        assert "คำเตือนสัดส่วน Margin เกิน 100% (8 × 20% = 160%)" in set_html
        assert set_kb[0][2]["text"] == "✅8ไม้"
        assert set_kb[1][3]["text"] == "✅20%"
        assert set_kb[2][4]["text"] == "✅20x"
        assert set_kb[3][1]["text"] == "✅ เฉพาะ 3⭐ A+"

        # Reset portfolio preserves custom Telegram settings!
        st_reset = pt.reset_portfolio()
        assert len(st_reset["open_positions"]) == 0
        assert PaperTrader.state_has_custom_settings(st_reset) is True
        assert PaperTrader.state_effective_max_open_positions(st_reset) == 8

        # Reload from disk preserves overrides
        pt_reloaded = PaperTrader(
            state_file=os.path.join(tmpdir, "state.json"),
            csv_file=os.path.join(tmpdir, "trades.csv"),
        )
        snap_reloaded = pt_reloaded.get_state_snapshot()
        assert PaperTrader.state_effective_max_open_positions(snap_reloaded) == 8
        assert abs(PaperTrader.state_effective_leverage(snap_reloaded) - 20.0) < 1e-6

        # /set command parser checks
        assert parse_set_command_args("default") == ("reset", None)
        mode, parsed_vals = parse_set_command_args("max 10 size 12.5% lev=15x stars 2")
        assert mode == "update"
        assert parsed_vals["max_open_positions"] == 10
        assert abs(parsed_vals["position_pct"] - 12.5) < 1e-6
        assert abs(parsed_vals["leverage"] - 15.0) < 1e-6
        assert parsed_vals["min_stars"] == 2
        mode_err, _ = parse_set_command_args("lev")
        assert mode_err == "error"

        # Revert to .env defaults
        st_default = pt_reloaded.clear_runtime_settings()
        assert PaperTrader.state_has_custom_settings(st_default) is False
        assert PaperTrader.state_effective_max_open_positions(st_default) == PAPER_MAX_OPEN_POSITIONS

        print("✅ ทดสอบตรรกะระบบจำลองการเทรด + Telegram Dashboard & Settings Self-Test (5 กรณี) ผ่าน 100%!")


def main():
    tg_bot_txt = (
        "เปิด ✅ (แจ้งเตือน + พอร์ต & ปุ่มสั่งการ)"
        if telegram_bot.is_enabled()
        else ("เปิด ✅ (แจ้งเตือนอย่างเดียว)" if (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) else "ไม่ได้ตั้งค่า ⚪")
    )
    print("==============================================================")
    print("📱 Scanner-f (MTF Edition: 5m + 15m + 1h) — Binance Futures")
    print("• ชุดวิเคราะห์ : 5m (จุดเข้า) + 15m (โมเมนตัม/SL) + 1h (เทรนด์หลัก/TP3)")
    print(f"• เกณฑ์คัดกรอง : 24h Vol >= ${fmt_usd(MIN_QUOTE_VOL_24H)} | คัดเฉพาะ >= {MIN_STARS} ดาว (⭐⭐/⭐⭐⭐)")
    print(
        f"• จำลองการเทรด : {'เปิด ✅' if PAPER_TRADING_ENABLED else 'ปิด ⚪'} | ทุน ${PAPER_INITIAL_BALANCE:.0f} | "
        f"{PAPER_LEVERAGE:.0f}x | ไม้ละ {PAPER_POSITION_PCT:.0f}% | Max {PAPER_MAX_OPEN_POSITIONS} ไม้"
    )
    print(f"• แจ้งเตือน   : Telegram={tg_bot_txt} | Discord={'เปิด ✅' if DISCORD_WEBHOOK_URL else 'ไม่ได้ตั้งค่า ⚪'}")
    print("==============================================================")

    if "--paper-self-test" in sys.argv:
        run_paper_self_test()
        return

    if "--paper-reset" in sys.argv:
        paper_trader.reset_portfolio()
        print(f"🔄 รีเซ็ตพอร์ตจำลองการเทรดกลับเป็น ${PAPER_INITIAL_BALANCE:.2f} USDT เรียบร้อย!")
        return

    if "--paper-status" in sys.argv:
        if paper_trader.get_state_snapshot()["open_positions"]:
            try:
                pmap = fetch_all_prices()
                paper_trader.update_prices(pmap)
            except Exception as e:
                print(f"⚠️ อัปเดตราคาตลาดล่าสุดไม่ได้ (แสดงข้อมูลล่าสุดที่บันทึกไว้): {e}")
        paper_trader.print_portfolio_summary()
        if "--send" in sys.argv:
            telegram_bot.send_dashboard_message(
                refresh_prices=False,
                notice="💼 รายงานพอร์ตจำลองตามคำสั่ง",
            )
        return

    if "--test-alert" in sys.argv:
        dummy_5m = {"label": "🟢 ขาขึ้น (EMA9>21, RSI 24.5) [Oversold 🧊]", "vol_ratio": 2.8, "atr": 180.0, "candles": []}
        dummy_15m = {"label": "🟢 พักตัวขาขึ้น (RSI 27.2) [Oversold 🧊]", "vol_ratio": 1.9, "atr": 350.0, "candles": []}
        dummy_1h = {"label": "🟢 ขาขึ้น (EMA9>21, RSI 54.0)", "vol_ratio": 1.4, "atr": 720.0, "candles": []}
        dispatch_mtf_alert(
            "BTCUSDT",
            "TEST_MTF",
            "🧊 RSI เข้าเขตขายมากเกินไป (Oversold)",
            "BULL",
            95420.50,
            4.85,
            18_500_000_000.0,
            "5m",
            "RSI(14) แท่ง 5m = 24.50 (<= 28) ย่อตัวลงแนวรับ",
            dummy_5m,
            dummy_15m,
            dummy_1h,
            "⭐⭐⭐ (3/3 ดาว — A+ Setup สอดคล้องครบ 5m+15m+1h)",
            True,
            ["15m เข้าเขต Oversold ร่วมด้วย (RSI 27.2)", "ตามเทรนด์ใหญ่ 1h (ขาขึ้น)"],
            force=True,
        )
        telegram_bot.send_dashboard_message(
            refresh_prices=False,
            notice="🧪 ทดสอบแสดงผลพอร์ตจำลอง + ปุ่มสั่งการบน Telegram",
        )
        return

    if "--once" in sys.argv:
        with scan_in_progress_lock:
            scan_once()
        telegram_bot.send_dashboard_message(
            refresh_prices=False,
            notice="📊 สรุปพอร์ตจำลองหลังสแกนรอบล่าสุด",
        )
        print("\n✅ สแกนครบ 1 รอบเรียบร้อยแล้ว")
        return

    telegram_bot.start()
    print(f"🟢 เริ่มสแกน MTF (5m + 15m + 1h) อัตโนมัติทุกๆ {SCAN_INTERVAL_SECS} วินาที (กด Ctrl+C เพื่อหยุด)...")
    sent_startup_dashboard = False
    while True:
        try:
            with scan_in_progress_lock:
                scan_once()
            if not sent_startup_dashboard:
                telegram_bot.send_dashboard_message(
                    refresh_prices=False,
                    notice="🚀 เริ่มทำงานสแกนตลาด + จำลองการเทรดแล้ว",
                )
                sent_startup_dashboard = True
        except KeyboardInterrupt:
            print("\n🛑 หยุดการทำงานเรียบร้อย")
            break
        except Exception as e:
            print(f"⚠️ เกิดข้อผิดพลาดในการสแกน: {e}")

        try:
            scan_now_event.wait(timeout=SCAN_INTERVAL_SECS)
            scan_now_event.clear()
        except KeyboardInterrupt:
            print("\n🛑 หยุดการทำงานเรียบร้อย")
            break


if __name__ == "__main__":
    main()
