#!/usr/bin/env python3
"""
📱 Scanner-f Mobile Edition (Zero-Dependency Python 3 Script)
รันได้ทันทีบนมือถือทุกรุ่น (Android: Termux / Pydroid 3, iOS: a-Shell / Pythonista)
พร้อมคำนวณจุดเข้า (Entry), เป้าทำกำไร (TP1 / TP2) และจุดตัดขาดทุน (Stop Loss) อัตโนมัติด้วย ATR & Swing High/Low
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
KLINE_INTERVAL = os.environ.get("KLINE_INTERVAL", "5m")
CANDLE_LIMIT = int(os.environ.get("CANDLE_BUFFER_SIZE", "60"))

PRICE_CHANGE_PCT = float(os.environ.get("PRICE_CHANGE_THRESHOLD_PCT", "2.5"))
VOL_SPIKE_MULT = float(os.environ.get("VOLUME_SPIKE_MULTIPLIER", "3.5"))
VOL_LOOKBACK = int(os.environ.get("VOLUME_LOOKBACK", "20"))

RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))
RSI_OB = float(os.environ.get("RSI_OVERBOUGHT", "75.0"))
RSI_OS = float(os.environ.get("RSI_OVERSOLD", "25.0"))

EMA_FAST = int(os.environ.get("EMA_FAST_PERIOD", "9"))
EMA_SLOW = int(os.environ.get("EMA_SLOW_PERIOD", "21"))

BB_PERIOD = int(os.environ.get("BB_PERIOD", "20"))
BB_STD = float(os.environ.get("BB_STD_DEV", "2.0"))
BB_MIN_VOL_RATIO = float(os.environ.get("BB_MIN_VOLUME_RATIO", "1.8"))

FUNDING_THRESHOLD_PCT = float(os.environ.get("FUNDING_RATE_THRESHOLD_PCT", "0.10"))
SCAN_INTERVAL_SECS = int(os.environ.get("FUTURES_POLL_INTERVAL_SECS", "60"))
ALERT_COOLDOWN_SECS = int(os.environ.get("ALERT_COOLDOWN_SECS", "900"))

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

cooldown_cache = {}


def http_get_json(url, timeout=12):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "scanner-f-mobile/1.1"},
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
            "User-Agent": "scanner-f-mobile/1.1",
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
    if len(candles) < 2:
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


def build_trade_plan(direction, price, candles=None):
    """
    คำนวณแผนเทรด: จุดเข้า (Entry), จุดตัดขาดทุน (SL), และเป้าทำกำไร (TP1 1.5R, TP2 2.5R)
    โดยอิงจากค่า ATR(14) และแนวรับ/แนวต้าน (Swing High/Low) ล่าสุด
    """
    if price <= 0:
        return None

    atr = calc_atr(candles, 14) if candles else 0.0
    if atr <= 0:
        atr = price * 0.012  # fallback 1.2%

    # ระยะความเสี่ยง (Risk Distance) มาตรฐาน = 1.5 * ATR
    # จำกัดขอบเขตความเสี่ยงให้อยู่ระหว่าง 0.6% ถึง 4.5% ของราคาปัจจุบัน
    raw_risk = atr * 1.5
    if candles and len(candles) >= 6:
        recent = candles[-10:] if len(candles) >= 10 else candles
        if direction == "BULL":
            swing_low = min(c["low"] for c in recent)
            swing_dist = (price - swing_low) + (0.25 * atr)
            if swing_dist > 0:
                raw_risk = max(raw_risk, swing_dist)
        elif direction == "BEAR":
            swing_high = max(c["high"] for c in recent)
            swing_dist = (swing_high - price) + (0.25 * atr)
            if swing_dist > 0:
                raw_risk = max(raw_risk, swing_dist)

    min_risk = price * 0.006  # ขั้นต่ำ 0.6%
    max_risk = price * 0.045  # สูงสุด 4.5%
    risk = max(min_risk, min(max_risk, raw_risk))
    risk_pct = (risk / price) * 100.0

    if direction == "BULL":
        entry_low = price - (0.25 * atr)
        sl = price - risk
        tp1 = price + (risk * 1.5)
        tp2 = price + (risk * 2.5)
        tp1_pct = ((tp1 - price) / price) * 100.0
        tp2_pct = ((tp2 - price) / price) * 100.0
        return {
            "side_label": "🟢 เปิด LONG (เก็งกำไรขาขึ้น)",
            "entry_str": f"${fmt_price(entry_low)} - ${fmt_price(price)}",
            "tp1_str": f"${fmt_price(tp1)} (+{tp1_pct:.2f}%)",
            "tp2_str": f"${fmt_price(tp2)} (+{tp2_pct:.2f}%)",
            "sl_str": f"${fmt_price(sl)} (-{risk_pct:.2f}%)",
            "rr_str": "1 : 1.5 / 1 : 2.5",
        }
    else:
        entry_high = price + (0.25 * atr)
        sl = price + risk
        tp1 = price - (risk * 1.5)
        tp2 = price - (risk * 2.5)
        tp1_pct = ((price - tp1) / price) * 100.0
        tp2_pct = ((price - tp2) / price) * 100.0
        return {
            "side_label": "🔴 เปิด SHORT (เก็งกำไรขาลง)",
            "entry_str": f"${fmt_price(price)} - ${fmt_price(entry_high)}",
            "tp1_str": f"${fmt_price(tp1)} (+{tp1_pct:.2f}%)",
            "tp2_str": f"${fmt_price(tp2)} (+{tp2_pct:.2f}%)",
            "sl_str": f"${fmt_price(sl)} (-{risk_pct:.2f}%)",
            "rr_str": "1 : 1.5 / 1 : 2.5",
        }


def dispatch_alert(
    symbol,
    category_key,
    title_th,
    direction,
    price,
    chg24,
    vol24,
    tf,
    summary,
    details,
    candles=None,
    force=False,
):
    now = time.time()
    cd_key = f"{symbol}:{category_key}"
    if not force:
        last_ts = cooldown_cache.get(cd_key, 0)
        if now - last_ts < ALERT_COOLDOWN_SECS:
            return
        cooldown_cache[cd_key] = now

    plan = build_trade_plan(direction, price, candles)

    ts_str = datetime.now().strftime("%H:%M:%S")
    detail_str = " | ".join(f"{k}={v}" for k, v in details)
    print(f"\n🔔 [{ts_str}] {title_th}")
    print(f"   🪙 #{symbol} ({tf}) | ราคา: ${fmt_price(price)} | 24h: {chg24:+.2f}% | Vol 24h: ${fmt_usd(vol24)}")
    print(f"   📌 {summary}")
    if detail_str:
        print(f"   📊 {detail_str}")
    if plan:
        print(f"   🧭 แผนเทรด : {plan['side_label']}")
        print(f"   🎯 จุดเข้า (Entry) : {plan['entry_str']}")
        print(f"   💰 เป้า TP1 / TP2  : {plan['tp1_str']}  |  {plan['tp2_str']}")
        print(f"   🛡️ ตัดขาดทุน (SL)  : {plan['sl_str']}  (R:R {plan['rr_str']})")

    futures_url = f"https://www.binance.com/en/futures/{symbol}"

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        lines = [
            f"<b>{html.escape(title_th)}</b>",
            f"🪙 เหรียญ: <b>#{html.escape(symbol)}</b> (TF: <code>{html.escape(tf)}</code>)",
            f"💵 ราคาปัจจุบัน: <code>${fmt_price(price)}</code>",
            f"📊 24h Change: <code>{chg24:+.2f}%</code> | 💧 Vol: <code>${fmt_usd(vol24)}</code>",
            f"📌 สัญญาณ: <b>{html.escape(summary)}</b>",
        ]
        for k, v in details:
            lines.append(f"• {html.escape(str(k))}: <code>{html.escape(str(v))}</code>")

        if plan:
            lines.append("")
            lines.append(f"🧭 <b>แผนเทรด: {html.escape(plan['side_label'])}</b>")
            lines.append(f"🎯 จุดเข้า (Entry): <code>{html.escape(plan['entry_str'])}</code>")
            lines.append(f"💰 เป้าทำกำไร TP1: <code>{html.escape(plan['tp1_str'])}</code>")
            lines.append(f"💰 เป้าทำกำไร TP2: <code>{html.escape(plan['tp2_str'])}</code>")
            lines.append(f"🛡️ จุดตัดขาดทุน SL: <code>{html.escape(plan['sl_str'])}</code>")

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
            {"name": "🪙 เหรียญ", "value": f"**{symbol}** (`{tf}`)", "inline": True},
            {"name": "💵 ราคา", "value": f"`${fmt_price(price)}`", "inline": True},
            {"name": "📊 24h Change / Vol", "value": f"`{chg24:+.2f}%` / `${fmt_usd(vol24)}`", "inline": True},
            {"name": "📌 สัญญาณ", "value": f"**{summary}**", "inline": False},
        ]
        if plan:
            fields.extend(
                [
                    {"name": "🧭 ทิศทาง", "value": f"**{plan['side_label']}**", "inline": False},
                    {"name": "🎯 จุดเข้า (Entry)", "value": f"`{plan['entry_str']}`", "inline": True},
                    {"name": "🛡️ ตัดขาดทุน (SL)", "value": f"`{plan['sl_str']}`", "inline": True},
                    {"name": "💰 เป้าทำกำไร (TP1 / TP2)", "value": f"TP1: `{plan['tp1_str']}`\nTP2: `{plan['tp2_str']}`", "inline": False},
                ]
            )
        for k, v in details:
            fields.append({"name": str(k), "value": f"`{v}`", "inline": True})
        try:
            http_post_json(
                DISCORD_WEBHOOK_URL,
                {
                    "username": "Scanner-f Mobile",
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


def fetch_symbol_klines(symbol):
    url = f"{FAPI_URL}/fapi/v1/klines?symbol={symbol}&interval={KLINE_INTERVAL}&limit={CANDLE_LIMIT}"
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
    return symbol, candles


def scan_once():
    print(f"\n⏳ [{datetime.now().strftime('%H:%M:%S')}] กำลังดึงข้อมูลตลาด Binance USDT-M Futures...")
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
    print(f"✅ พบเหรียญผ่านเกณฑ์ Volume >= ${fmt_usd(MIN_QUOTE_VOL_24H)} จำนวน {len(symbols)} เหรียญ")

    # 1. สแกน Funding Rate
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
                dispatch_alert(
                    sym,
                    "FUNDING",
                    "💰 Funding Rate สูง/ต่ำผิดปกติ",
                    direction,
                    float(item.get("markPrice", meta["price"])),
                    meta["chg24"],
                    meta["vol24"],
                    "Funding",
                    f"Funding Rate = {fr_pct:+.4f}% ({note})",
                    [("Funding Rate", f"{fr_pct:+.4f}%")],
                )
    except Exception as e:
        print(f"⚠️ ดึงข้อมูล Funding Rate ไม่สำเร็จ: {e}")

    # 2. สแกนแท่งเทียน & อินดิเคเตอร์แบบขนาน
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(fetch_symbol_klines, sym) for sym in symbols]
        for fut in concurrent.futures.as_completed(futures):
            try:
                sym, candles = fut.result()
            except Exception:
                continue
            if len(candles) < VOL_LOOKBACK + 2:
                continue

            meta = ticker_map[sym]
            latest = candles[-1]
            open_p = latest["open"]
            close_p = latest["close"]
            chg_pct = ((close_p - open_p) / open_p * 100.0) if open_p > 0 else 0.0

            prev_vols = [c["quote_vol"] for c in candles[-1 - VOL_LOOKBACK : -1]]
            avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0.0
            vol_ratio = (latest["quote_vol"] / avg_vol) if avg_vol > 0 else 1.0

            # Price Pump / Dump
            if chg_pct >= PRICE_CHANGE_PCT:
                dispatch_alert(
                    sym,
                    "PUMP",
                    "🚀 ราคาพุ่งแรง (Price Pump)",
                    "BULL",
                    close_p,
                    meta["chg24"],
                    meta["vol24"],
                    KLINE_INTERVAL,
                    f"ราคาพุ่ง +{chg_pct:.2f}% ในแท่ง {KLINE_INTERVAL}",
                    [("Volume Ratio", f"{vol_ratio:.2f}x"), ("Open -> Now", f"${fmt_price(open_p)} -> ${fmt_price(close_p)}")],
                    candles=candles,
                )
            elif chg_pct <= -PRICE_CHANGE_PCT:
                dispatch_alert(
                    sym,
                    "DUMP",
                    "🩸 ราคาทุบแรง (Price Dump)",
                    "BEAR",
                    close_p,
                    meta["chg24"],
                    meta["vol24"],
                    KLINE_INTERVAL,
                    f"ราคาทุบ {chg_pct:.2f}% ในแท่ง {KLINE_INTERVAL}",
                    [("Volume Ratio", f"{vol_ratio:.2f}x"), ("Open -> Now", f"${fmt_price(open_p)} -> ${fmt_price(close_p)}")],
                    candles=candles,
                )

            # Volume Spike
            if vol_ratio >= VOL_SPIKE_MULT:
                dispatch_alert(
                    sym,
                    "VOL_SPIKE",
                    "📊 วอลุ่มพุ่งผิดปกติ (Volume Spike)",
                    "BULL" if chg_pct >= 0 else "BEAR",
                    close_p,
                    meta["chg24"],
                    meta["vol24"],
                    KLINE_INTERVAL,
                    f"วอลุ่มพุ่ง {vol_ratio:.2f} เท่าของค่าเฉลี่ย ({chg_pct:+.2f}%)",
                    [("Volume แท่งนี้", f"${fmt_usd(latest['quote_vol'])}"), ("ค่าเฉลี่ย", f"${fmt_usd(avg_vol)}")],
                    candles=candles,
                )

            # อินดิเคเตอร์คำนวณจากแท่งที่ปิดแล้ว
            closed_candles = candles[:-1] if not latest["closed"] else candles
            closes = [c["close"] for c in closed_candles]
            if len(closes) < max(EMA_SLOW + 2, BB_PERIOD, RSI_PERIOD + 1):
                continue

            eval_price = closes[-1]

            # RSI
            rsi = calc_rsi(closes, RSI_PERIOD)
            if rsi is not None:
                if rsi >= RSI_OB:
                    dispatch_alert(
                        sym,
                        "RSI_OB",
                        "🔥 RSI เข้าเขตซื้อมากเกินไป (Overbought)",
                        "BEAR",
                        eval_price,
                        meta["chg24"],
                        meta["vol24"],
                        KLINE_INTERVAL,
                        f"RSI({RSI_PERIOD}) = {rsi:.2f} (Overbought >= {RSI_OB:.0f})",
                        [(f"RSI({RSI_PERIOD})", f"{rsi:.2f}")],
                        candles=closed_candles,
                    )
                elif rsi <= RSI_OS:
                    dispatch_alert(
                        sym,
                        "RSI_OS",
                        "🧊 RSI เข้าเขตขายมากเกินไป (Oversold)",
                        "BULL",
                        eval_price,
                        meta["chg24"],
                        meta["vol24"],
                        KLINE_INTERVAL,
                        f"RSI({RSI_PERIOD}) = {rsi:.2f} (Oversold <= {RSI_OS:.0f})",
                        [(f"RSI({RSI_PERIOD})", f"{rsi:.2f}")],
                        candles=closed_candles,
                    )

            # EMA Crossover
            fast_emas = calc_ema_series(closes, EMA_FAST)
            slow_emas = calc_ema_series(closes, EMA_SLOW)
            if fast_emas and slow_emas:
                if fast_emas[-2] <= slow_emas[-2] and fast_emas[-1] > slow_emas[-1]:
                    dispatch_alert(
                        sym,
                        "EMA_BULL",
                        "📈 EMA Golden Cross (ตัดขึ้น)",
                        "BULL",
                        eval_price,
                        meta["chg24"],
                        meta["vol24"],
                        KLINE_INTERVAL,
                        f"EMA({EMA_FAST}) ตัดขึ้นเหนือ EMA({EMA_SLOW})",
                        [(f"EMA({EMA_FAST})", fmt_price(fast_emas[-1])), (f"EMA({EMA_SLOW})", fmt_price(slow_emas[-1]))],
                        candles=closed_candles,
                    )
                elif fast_emas[-2] >= slow_emas[-2] and fast_emas[-1] < slow_emas[-1]:
                    dispatch_alert(
                        sym,
                        "EMA_BEAR",
                        "📉 EMA Death Cross (ตัดลง)",
                        "BEAR",
                        eval_price,
                        meta["chg24"],
                        meta["vol24"],
                        KLINE_INTERVAL,
                        f"EMA({EMA_FAST}) ตัดลงต่ำกว่า EMA({EMA_SLOW})",
                        [(f"EMA({EMA_FAST})", fmt_price(fast_emas[-1])), (f"EMA({EMA_SLOW})", fmt_price(slow_emas[-1]))],
                        candles=closed_candles,
                    )

            # Bollinger Bands Breakout
            bb = calc_bollinger(closes, BB_PERIOD, BB_STD)
            if bb is not None and vol_ratio >= BB_MIN_VOL_RATIO:
                upper, mid, lower = bb
                if eval_price > upper:
                    dispatch_alert(
                        sym,
                        "BB_UPPER",
                        "⚡ ทะลุกรอบบน Bollinger Band + Volume",
                        "BULL",
                        eval_price,
                        meta["chg24"],
                        meta["vol24"],
                        KLINE_INTERVAL,
                        f"ราคาปิดทะลุ Upper BB พร้อมวอลุ่ม {vol_ratio:.2f}x",
                        [("Upper BB", f"${fmt_price(upper)}"), ("Volume Ratio", f"{vol_ratio:.2f}x")],
                        candles=closed_candles,
                    )
                elif eval_price < lower:
                    dispatch_alert(
                        sym,
                        "BB_LOWER",
                        "⚠️ หลุดกรอบล่าง Bollinger Band + Volume",
                        "BEAR",
                        eval_price,
                        meta["chg24"],
                        meta["vol24"],
                        KLINE_INTERVAL,
                        f"ราคาปิดหลุด Lower BB พร้อมวอลุ่ม {vol_ratio:.2f}x",
                        [("Lower BB", f"${fmt_price(lower)}"), ("Volume Ratio", f"{vol_ratio:.2f}x")],
                        candles=closed_candles,
                    )


def main():
    print("==============================================================")
    print("📱 Scanner-f (Mobile Edition) — สแกนเนอร์ Binance Futures")
    print(f"• Timeframe: {KLINE_INTERVAL} | Min 24h Vol: ${fmt_usd(MIN_QUOTE_VOL_24H)} | Top {MAX_SYMBOLS} เหรียญ")
    print(f"• Telegram: {'เปิด ✅' if (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) else 'ไม่ได้ตั้งค่า ⚪'} | Discord: {'เปิด ✅' if DISCORD_WEBHOOK_URL else 'ไม่ได้ตั้งค่า ⚪'}")
    print("==============================================================")

    if "--test-alert" in sys.argv:
        dispatch_alert(
            "BTCUSDT",
            "TEST",
            "🚀 ทดสอบระบบแจ้งเตือนบนมือถือ (Scanner-f Mobile)",
            "BULL",
            95420.50,
            4.85,
            18_500_000_000.0,
            KLINE_INTERVAL,
            "RSI(14) = 23.13 (Oversold <= 25) พร้อมสัญญาณกลับตัว",
            [("RSI(14)", "23.13"), ("EMA(9/21)", "Golden Cross")],
            force=True,
        )
        return

    if "--once" in sys.argv:
        scan_once()
        print("\n✅ สแกนครบ 1 รอบเรียบร้อยแล้ว")
        return

    print(f"🟢 เริ่มสแกนอัตโนมัติทุกๆ {SCAN_INTERVAL_SECS} วินาที (กด Ctrl+C เพื่อหยุด)...")
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
