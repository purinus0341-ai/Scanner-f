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
import html
import json
import math
import os
import sys
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
        }


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
            return
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

        lines.append(f"\n🔗 <a href=\"{futures_url}\">เปิดกราฟ Binance Futures ({html.escape(symbol)})</a>")
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
    confirmed_Results = []

    for tf_name, (cat_key, title_th, direction, is_reversal, summary) in all_raw:
        if cat_key in seen_categories:
            continue
        stars, grade_str, aligned_1h, reasons = evaluate_mtf_confluence(
            direction, is_reversal, state_5m, state_15m, state_1h
        )
        if stars >= MIN_STARS:
            seen_categories.add(cat_key)
            confirmed_Results.append(
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

    return symbol, confirmed_Results


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
    qualified = []
    ticker_map = {}
    for t in tickers:
        sym = t["symbol"]
        if sym not in valid_perps:
            continue
        qvol = float(t.get("quoteVolume", 0.0))
        if qvol >= MIN_QUOTE_VOL_24H:
            qualified.append((qvol, sym))
            ticker_map[sym] = {
                "price": float(t.get("lastPrice", 0.0)),
                "chg24": float(t.get("priceChangePercent", 0.0)),
                "vol24": qvol,
            }

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
                dispatch_mtf_alert(
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
                    dispatch_mtf_alert(
                        sym,
                        "FUNDING",
                        "💰 Funding Rate สูง/ต่ำผิดปกติ",
                        direction,
                        float(item.get("markPrice", meta["price"])),
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
    except Exception as e:
        print(f"⚠️ ดึงข้อมูล Funding Rate ไม่สำเร็จ: {e}")

    print(f"🏁 จบรอบสแกน MTF: พบสัญญาณคุณภาพ (⭐⭐ ขึ้นไป) จำนวน {signal_total} รายการ")


def main():
    print("==============================================================")
    print("📱 Scanner-f (MTF Edition: 5m + 15m + 1h) — Binance Futures")
    print("• ชุดวิเคราะห์ : 5m (จุดเข้า) + 15m (โมเมนตัม/SL) + 1h (เทรนด์หลัก/TP3)")
    print(f"• เกณฑ์คัดกรอง : 24h Vol >= ${fmt_usd(MIN_QUOTE_VOL_24H)} | คัดเฉพาะ >= {MIN_STARS} ดาว (⭐⭐/⭐⭐⭐)")
    print(f"• แจ้งเตือน   : Telegram={'เปิด ✅' if (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) else 'ไม่ได้ตั้งค่า ⚪'} | Discord={'เปิด ✅' if DISCORD_WEBHOOK_URL else 'ไม่ได้ตั้งค่า ⚪'}")
    print("==============================================================")

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
        return

    if "--once" in sys.argv:
        scan_once()
        print("\n✅ สแกนครบ 1 รอบเรียบร้อยแล้ว")
        return

    print(f"🟢 เริ่มสแกน MTF (5m + 15m + 1h) อัตโนมัติทุกๆ {SCAN_INTERVAL_SECS} วินาที (กด Ctrl+C เพื่อหยุด)...")
    while True:
        try:
            scan_once()
        except KeyboardInterrupt:
            print("\n🛑 หยุดการทำงานเรียบร้อย")
            break
        except Exception as e:
            print(f"⚠️ เกิดข้อผิดพลาดในการสแกน: {e}")
        time.sleep(SCAN_INTERVAL_SECS)


if __name__ == "__main__":
    main()
