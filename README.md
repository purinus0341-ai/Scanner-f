# ⚡ Scanner-f — ระบบสแกนเนอร์ Binance USDT-M Crypto Futures (Rust)

ระบบสแกนตลาด **Binance USDT-M Perpetual Futures** แบบเรียลไทม์ พัฒนาด้วยภาษา **Rust** (`tokio`, `tokio-tungstenite`, `reqwest`) ออกแบบมาเพื่อตรวจจับความผิดปกติของราคา วอลุ่ม อินดิเคเตอร์ทางเทคนิค และข้อมูลเชิงลึกของตลาด Futures พร้อมส่งแจ้งเตือนผ่าน **Telegram Bot** และ **Discord Webhook** ทันทีที่เข้าเงื่อนไข

---

## 🎯 คุณสมบัติหลัก (Features)

1. **การคัดกรองเหรียญอัตโนมัติ (Dynamic Universe Filter)**
   - ดึงทุกคู่เหรียญในตลาด **USDT-M Perpetual Futures** ที่สถานะเป็น `TRADING`
   - กรองเฉพาะเหรียญที่มีสภาพคล่องสูงด้วยเกณฑ์ **24h Quote Volume ขั้นต่ำ** (`MIN_QUOTE_VOLUME_24H`, ค่าเริ่มต้น `20,000,000 USDT`)

2. **สแกนความผันผวนของราคา & วอลุ่มพุ่ง (Real-time Price Pump/Dump & Volume Spike)**
   - **Price Pump / Dump**: แจ้งเตือนเมื่อราคาในแท่งเทียนปัจจุบันพุ่งขึ้นหรือทุบลงเกินเปอร์เซ็นต์ที่กำหนด (`PRICE_CHANGE_THRESHOLD_PCT`)
   - **Volume Spike**: แจ้งเตือนเมื่อ Quote Volume ในแท่งปัจจุบันพุ่งสูงกว่าค่าเฉลี่ยย้อนหลังเกินตัวคูณที่กำหนด (`VOLUME_SPIKE_MULTIPLIER`)

3. **อินดิเคเตอร์ทางเทคนิค (Technical Indicators)**
   - **RSI (Wilder's Smoothing)**: ตรวจจับภาวะซื้อมากเกินไป (`RSI_OVERBOUGHT`) และขายมากเกินไป (`RSI_OVERSOLD`)
   - **EMA Crossover**: ตรวจจับสัญญาณตัดกันของเส้นค่าเฉลี่ยเคลื่อนที่เร็วและช้า (`EMA_FAST_PERIOD` / `EMA_SLOW_PERIOD` เช่น EMA 9 ตัด EMA 21)
   - **Bollinger Bands Breakout**: ตรวจจับเมื่อราคาปิดทะลุกรอบบนหรือหลุดกรอบล่างของ Bollinger Bands พร้อมกับมี Relative Volume ยืนยัน

4. **ข้อมูล Futures เฉพาะทาง (Futures-Specific Metrics)**
   - **Funding Rate สูง/ต่ำผิดปกติ**: สแกนหาเหรียญที่ Funding Rate เป็นบวกสูงหรือติดลบหนัก (`FUNDING_RATE_THRESHOLD_PCT`) ซึ่งมักเป็นสัญญาณเตือน Short Squeeze หรือ Long Crowded
   - **Open Interest (OI) Spike**: ตรวจจับการไหลเข้า/ออกของเม็ดเงินสัญญาคงค้างอย่างฉับพลัน (`OI_SPIKE_THRESHOLD_PCT`)
   - **Real-time Liquidation (`!forceOrder@arr`)**: ดักจับการถูกบังคับล้างพอร์ต (Long/Short Liquidated) รายใหญ่แบบเรียลไทม์เมื่อมูลค่าเกิน `MIN_LIQUIDATION_USD`

5. **ระบบแจ้งเตือนพร้อมป้องกันสแปม (Telegram & Discord Webhook + Cooldown & State Cache)**
   - รองรับทั้ง **Telegram Bot API** (รูปแบบ HTML พร้อมลิงก์เปิดกราฟ Binance Futures) และ **Discord Webhook** (Rich Embed แยกสีเขียว/แดงตามทิศทางสัญญาณ)
   - มีระบบ **Cooldown ต่อเหรียญและประเภทสัญญาณ** (`ALERT_COOLDOWN_SECS`) และบันทึกสถานะลง `.scanner_state.json` เพื่อจดจำค่า Open Interest และ Cooldown ข้ามรอบการทำงาน

---

## ☁️ วิธีรันผ่าน GitHub Actions โดยตรง

โปรเจกต์นี้มีไฟล์เวิร์กโฟลว์ [`.github/workflows/scanner.yml`](.github/workflows/scanner.yml) พร้อมใช้งานทันที:

### ขั้นตอนที่ 1: ตั้งค่า Reverse Proxy นอกสหรัฐฯ (แก้ HTTP 451 บน GitHub Runner)
เนื่องจาก GitHub Actions Runner ตั้งอยู่ในสหรัฐอเมริกา จึงถูก Binance Futures บล็อก (`HTTP 451`) ให้นำไฟล์ Reverse Proxy ในโฟลเดอร์ `proxy/` ไป Deploy บนเซิร์ฟเวอร์หรือบริการนอกสหรัฐฯ (แนะนำภูมิภาค **สิงคโปร์ / ญี่ปุ่น / ยุโรป**):
- **Deno / Fly.io (แนะนำ สำหรับรองรับทั้ง REST & WebSocket)**: ใช้ [`proxy/deno-proxy.ts`](proxy/deno-proxy.ts)
- **VPS (Nginx)**: ใช้ [`proxy/nginx.conf`](proxy/nginx.conf)
- **Cloudflare Workers**: ใช้ [`proxy/cloudflare-worker.js`](proxy/cloudflare-worker.js) *(หมายเหตุ: Free Plan ของ Cloudflare มักรัน Worker ที่ Edge PoP ใกล้ตัว GitHub Runner ในสหรัฐฯ ซึ่งอาจยังติด 451 จาก Binance ได้ หากใช้ Cloudflare แนะนำเปิด Smart Placement หรือใช้ VPS/Fly.io โซนสิงคโปร์แทน)*

### ขั้นตอนที่ 2: ตั้งค่า GitHub Secrets สำหรับแจ้งเตือนและ Proxy
ไปที่หน้า Repository บน GitHub → **Settings** → **Secrets and variables** → **Actions** → กด **New repository secret** แล้วเพิ่มค่าที่คุณต้องการใช้:

| ชื่อ Secret | คำอธิบาย |
| :--- | :--- |
| `BINANCE_FAPI_URL` | URL ของ Reverse Proxy สำหรับ REST API (เช่น `https://your-proxy.example.com`) |
| `BINANCE_FSTREAM_URL` | URL ของ Reverse Proxy สำหรับ WebSocket (เช่น `wss://your-proxy.example.com`) |
| `TELEGRAM_BOT_TOKEN` | Token ของ Telegram Bot จาก `@BotFather` (เช่น `123456:ABC-DEF...`) |
| `TELEGRAM_CHAT_ID` | Chat ID หรือ Channel/Group ID ที่ต้องการให้บอทส่งแจ้งเตือน |
| `DISCORD_WEBHOOK_URL` | Webhook URL ของห้อง Discord ที่ต้องการรับแจ้งเตือน |
| `HTTPS_PROXY` *(ทางเลือก)* | กรณีใช้ HTTP/HTTPS Proxy แทน Reverse Proxy |

### ขั้นตอนที่ 3: กดรันจากแท็บ Actions บน GitHub
1. ไปที่แท็บ **Actions** ของ Repository บน GitHub
2. เลือกเวิร์กโฟลว์ **Binance Futures Scanner (Scanner-f)** ด้านซ้ายมือ
3. กดปุ่ม **Run workflow** แล้วเลือกโหมดที่ต้องการ:
   - `test-alert`: ทดสอบส่งข้อความแจ้งเตือนเข้า Telegram / Discord ทันที
   - `once`: สแกนทุกเหรียญในตลาด 1 รอบแล้วจบการทำงาน (นอกจากนี้ระบบตั้งเวลา `cron` ให้รันอัตโนมัติทุกๆ 15 นาที)
   - `realtime`: เปิดเชื่อมต่อ WebSocket สแกนแบบเรียลไทม์ต่อเนื่องตามจำนวนนาทีที่กำหนด (`duration_mins`, สูงสุด 350 นาทีต่อรอบ)

---

## 💻 วิธีรันบนเครื่องหรือเซิร์ฟเวอร์ส่วนตัว (Local / VPS)

```bash
# 1. คัดลอกไฟล์ตั้งค่า
cp .env.example .env

# 2. ทดสอบส่งแจ้งเตือน
cargo run --release -- --test-alert

# 3. สแกนตลาดแบบรอบเดียว
cargo run --release -- --once

# 4. รันสแกนแบบเรียลไทม์ 24/7
cargo run --release
```
