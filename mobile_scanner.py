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
        try:
            http_post_json(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": "\n".join(msg_lines),
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
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


class PaperTrader:
    def __init__(self, state_file=None, csv_file=None):
        self.state_file = state_file or PAPER_STATE_FILE
        self.csv_file = csv_file or PAPER_TRADES_CSV
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
                        return base
            except Exception:
                pass
        return base

    def save_state(self):
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️ บันทึกพอร์ตจำลองไม่สำเร็จ: {e}")

    def reset_portfolio(self):
        self.state = self._new_state()
        self.save_state()
        if os.path.exists(self.csv_file):
            try:
                os.remove(self.csv_file)
            except Exception:
                pass
        return self.state

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

    def locked_margin(self):
        return sum(p["remaining_margin"] for p in self.state["open_positions"])

    def unrealized_pnl(self):
        return sum(self.pos_unrealized_pnl(p) for p in self.state["open_positions"])

    def total_equity(self):
        return self.state["free_balance"] + self.locked_margin() + self.unrealized_pnl()

    def win_rate_pct(self):
        total = self.state["total_closed"]
        if total == 0:
            return 0.0
        return (self.state["win_count"] / total) * 100.0

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
        if not PAPER_TRADING_ENABLED or not plan or stars < PAPER_MIN_STARS:
            return None
        if direction not in ("BULL", "BEAR"):
            return None
        if len(self.state["open_positions"]) >= PAPER_MAX_OPEN_POSITIONS:
            return None
        if any(p["symbol"] == symbol for p in self.state["open_positions"]):
            return None

        entry_price = plan["entry_price"]
        if entry_price <= 0:
            return None

        side = "Long" if direction == "BULL" else "Short"
        equity = self.total_equity()
        desired_margin = equity * (PAPER_POSITION_PCT / 100.0)
        fee_rate = PAPER_TAKER_FEE_PCT / 100.0
        max_affordable_margin = self.state["free_balance"] / (1.0 + PAPER_LEVERAGE * fee_rate)
        margin = min(desired_margin, max_affordable_margin)
        if margin < 5.0:
            return None

        notional = margin * PAPER_LEVERAGE
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
            "leverage": PAPER_LEVERAGE,
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
            f"💰 Margin: ${margin:.2f} ({PAPER_LEVERAGE:.0f}x | มูลค่าสัญญา ${notional:.2f})",
            f"🎯 ราคาเข้า (Entry): ${fmt_price(entry_price)}",
            f"📊 เป้าปิด 3 ส่วน: TP1 ${fmt_price(plan['tp1'])} (40%) | TP2 ${fmt_price(plan['tp2'])} (30%) | TP3 ${fmt_price(plan['tp3'])} (30%)",
            f"🛡️ Stop Loss: ${fmt_price(plan['sl'])} (-{plan['risk_pct']:.2f}%) | ค่าธรรมเนียม: ${entry_fee:.2f}",
            f"💼 พอร์ตปัจจุบัน: Equity ${eq_after:.2f} | ถืออยู่ {len(self.state['open_positions'])}/{PAPER_MAX_OPEN_POSITIONS} ไม้",
        ]
        print(f"\n   {title}")
        for ln in lines_th:
            print(f"      {ln}")

        if notify:
            send_paper_webhook(title, symbol, lines_th, 0x00E676 if side == "Long" else 0xFF5252)

        return trade_id

    def update_prices(self, price_map, notify=True):
        if not PAPER_TRADING_ENABLED or not price_map or not self.state["open_positions"]:
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
                print(f"\n   {title}")
                for ln in lines_th:
                    print(f"      {ln}")
                if notify:
                    send_paper_webhook(title, sym, lines_th, 0x00E676 if pos["realized_pnl_trade"] >= 0 else 0xFF5252)
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
                print(f"\n   {title}")
                for ln in lines_th:
                    print(f"      {ln}")
                if notify:
                    send_paper_webhook(title, sym, lines_th, 0x00E676)

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
                print(f"\n   {title}")
                for ln in lines_th:
                    print(f"      {ln}")
                if notify:
                    send_paper_webhook(title, sym, lines_th, 0x00E676)

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
                print(f"\n   {title}")
                for ln in lines_th:
                    print(f"      {ln}")
                if notify:
                    send_paper_webhook(title, sym, lines_th, 0x00E676)
                continue

            idx += 1

        self.update_drawdown()
        self.save_state()

    def print_portfolio_summary(self):
        eq = self.total_equity()
        init_bal = self.state["initial_balance"]
        net_pct = ((eq - init_bal) / init_bal * 100.0) if init_bal > 0 else 0.0
        print("\n--------------------------------------------------------------")
        print(f"💼 พอร์ตจำลอง (Paper Trading) | Equity: ${eq:.2f} ({net_pct:+.2f}%) | ทุนตั้งต้น: ${init_bal:.2f}")
        print(f"   • เงินว่าง: ${self.state['free_balance']:.2f} | Margin ถืออยู่: ${self.locked_margin():.2f} | uPnL: {self.unrealized_pnl():+.2f} USDT")
        print(f"   • ปิดไม้แล้ว: {self.state['total_closed']} | Win Rate: {self.win_rate_pct():.1f}% ({self.state['win_count']}W/{self.state['loss_count']}L) | Realized: {self.state['realized_pnl']:+.2f} | Max DD: {self.state['max_drawdown_pct']:.2f}%")
        if not self.state["open_positions"]:
            print("   • ออเดอร์ที่ถืออยู่: ไม่มี (รอสัญญาณ ⭐⭐/⭐⭐⭐)")
        else:
            print(f"   • ออเดอร์ที่ถืออยู่ ({len(self.state['open_positions'])}/{PAPER_MAX_OPEN_POSITIONS}):")
            for p in self.state["open_positions"]:
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
        print("✅ ทดสอบตรรกะระบบจำลองการเทรด (Paper Trading Self-Test 3 กรณี) ผ่าน 100%!")


def main():
    print("==============================================================")
    print("📱 Scanner-f (MTF Edition: 5m + 15m + 1h) — Binance Futures")
    print("• ชุดวิเคราะห์ : 5m (จุดเข้า) + 15m (โมเมนตัม/SL) + 1h (เทรนด์หลัก/TP3)")
    print(f"• เกณฑ์คัดกรอง : 24h Vol >= ${fmt_usd(MIN_QUOTE_VOL_24H)} | คัดเฉพาะ >= {MIN_STARS} ดาว (⭐⭐/⭐⭐⭐)")
    print(
        f"• จำลองการเทรด : {'เปิด ✅' if PAPER_TRADING_ENABLED else 'ปิด ⚪'} | ทุน ${PAPER_INITIAL_BALANCE:.0f} | "
        f"{PAPER_LEVERAGE:.0f}x | ไม้ละ {PAPER_POSITION_PCT:.0f}% | Max {PAPER_MAX_OPEN_POSITIONS} ไม้"
    )
    print(f"• แจ้งเตือน   : Telegram={'เปิด ✅' if (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) else 'ไม่ได้ตั้งค่า ⚪'} | Discord={'เปิด ✅' if DISCORD_WEBHOOK_URL else 'ไม่ได้ตั้งค่า ⚪'}")
    print("==============================================================")

    if "--paper-self-test" in sys.argv:
        run_paper_self_test()
        return

    if "--paper-reset" in sys.argv:
        paper_trader.reset_portfolio()
        print(f"🔄 รีเซ็ตพอร์ตจำลองการเทรดกลับเป็น ${PAPER_INITIAL_BALANCE:.2f} USDT เรียบร้อย!")
        return

    if "--paper-status" in sys.argv:
        if os.path.exists(PAPER_STATE_FILE) and paper_trader.state["open_positions"]:
            try:
                tickers = http_get_json(f"{FAPI_URL}/fapi/v1/ticker/24hr")
                pmap = {t["symbol"]: float(t.get("lastPrice", 0.0)) for t in tickers}
                paper_trader.update_prices(pmap)
            except Exception as e:
                print(f"⚠️ อัปเดตราคาตลาดล่าสุดไม่ได้ (แสดงข้อมูลล่าสุดที่บันทึกไว้): {e}")
        paper_trader.print_portfolio_summary()
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
