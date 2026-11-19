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

5. **สแกน Trigger คู่ `5m + 15m` แบบเรียลไทม์ & วิเคราะห์หลาย Timeframe (`MTF` + Entry/TP/SL)**
   - เวอร์ชัน Rust เปิด WebSocket สแกนสัญญาณ Trigger คู่กันทั้ง **`5m` และ `15m` พร้อมกัน** โดยส่งทั้งสัญญาณระหว่างแท่ง (`Pump/Dump/Volume Spike`, กรองทิศทาง `>= 0.6%` และจำกัดรอบเช็กทุก `30s`) และสัญญาณปิดแท่ง (`RSI/EMA/BB`) เข้าคัดกรอง **MTF ครบชุด + Paper Trading** พร้อม **รีสแกนบัฟเฟอร์ MTF ใน RAM ซ้ำทุก 60 วินาที** (จับจังหวะเมื่อ `15m/1h` เพิ่งพลิกตามเทรนด์) แคชแท่ง `1h` (`300s`) และรายงาน **Heartbeat สถานะสแกนทุก 60 วินาที**
   - ให้คะแนนความแม่นยำ 1–3 ดาว (`⭐⭐` / `⭐⭐⭐`) โดยเชื่อมโยงจังหวะเข้า (`5m`), โมเมนตัมและโครงสร้าง Swing High/Low (`15m`), และเทรนด์ใหญ่ (`1h`)
   - คำนวณจุดเข้า-ออกครบชุดอัตโนมัติ: **Entry Zone (`5m`)**, **SL ฐานโครงสร้าง (`15m`)**, และเป้าหมาย 3 ระดับ (**TP1 `5m` / TP2 `15m` / TP3 `1h`**)

6. **ระบบเลือกกลยุทธ์การสแกน & จำลองการเทรด (5 Trading Strategy Presets)**
   - เลือกสไตล์การเทรดได้ 5 โปรไฟล์ ซึ่งปรับทั้ง **ตัวกรองประเภทสัญญาณ**, **เงื่อนไขบังคับตามเทรนด์ 1h**, **ระดับดาวขั้นต่ำ (Min⭐)**, **ระยะ SL / TP (R:R)** และ **สัดส่วนแบ่งปิด 3 ไม้** ให้เข้ากันแบบอัตโนมัติ (ตั้งค่าเริ่มต้นผ่าน `SCANNER_STRATEGY` หรือสลับสดผ่าน `/strategy` ใน Telegram ได้ทันที โดยไม้ที่เปิดค้างอยู่เดิมยังคงปิดตามแผนของกลยุทธ์เดิม):

   | กลยุทธ์ (`SCANNER_STRATEGY`) | สัญญาณที่รับ | เงื่อนไข 1h & ดาว | ระยะ SL (15m) | เป้า TP & แบ่งปิด | สไตล์การเล่น |
   | :--- | :--- | :---: | :---: | :---: | :--- |
   | **⚖️ `BALANCED`** *(ค่าเริ่มต้น)* | ทุกสัญญาณ (Pump, Vol, RSI, EMA, BB, Funding, OI, Liq) | ยืดหยุ่น 1h \| `.env` (2⭐) | `0.70%–4.80%` | `1.3R / 2.3R / 3.6R` (**40/30/30%**) | สมดุล ครบทุกสถานการณ์ |
   | **📈 `TREND_FOLLOW`** | EMA Cross, BB Breakout, Volume Spike | **บังคับตาม 1h** \| **3⭐** | `0.80%–4.80%` | `1.5R / 2.8R / 4.5R` (**25/35/40%**) | รันเทรนด์ใหญ่ เก็บคำโต TP2/TP3 |
   | **💥 `BREAKOUT`** | BB Breakout, Pump/Dump, Vol Spike (+ OI Spike) | ยืดหยุ่น 1h \| **2⭐** | `0.70%–4.20%` | `1.3R / 2.4R / 3.8R` (**35/35/30%**) | ดักทะลุกรอบ+โมเมนตัมแรง |
   | **🎯 `REVERSAL_SNIPER`** | RSI OB/OS, Funding Extreme (+ Liquidation) | ยืดหยุ่น 1h \| **3⭐** | `0.65%–3.80%` | `1.2R / 2.0R / 3.0R` (**50/35/15%**) | ดักจุดกลับตัวสุดขั้ว SL กระชับ |
   | **⚡ `SCALP_QUICK`** | EMA, RSI OB/OS, BB, Vol Spike, Pump/Dump | ยืดหยุ่น 1h \| **2⭐** | `0.55%–2.80%` | `1.0R / 1.7R / 2.5R` (**50/35/15%**) | เล่นรอบสั้นไว เน้นเก็บ TP1/TP2 |

   - เก็บ **สถิติ Win Rate / PnL แยกตามกลยุทธ์** (`strategy_stats` และคอลัมน์ที่ 20 `strategy` ใน `paper_trades.csv`) เพื่อให้เทียบผลงานแต่ละสายบนหน้า `/strategy` ได้ทันที

7. **ระบบจำลองการเทรดอัตโนมัติ (Paper Trading Simulator)**
   - เปิดออเดอร์จำลองให้อัตโนมัติเมื่อเกิดสัญญาณคุณภาพตามกลยุทธ์ที่เลือก โดยใช้ **ทุนจำลอง `$1,000 USDT`, Leverage `10x`, ไม้ละ `10%` ของพอร์ต, ถือพร้อมกันสูงสุด `5` ไม้** (ป้องกันเปิดซ้ำเหรียญเดิม) และหักค่าธรรมเนียมสมจริง (`PAPER_TAKER_FEE_PCT=0.05%` ต่อข้าง)
   - บริหารออเดอร์และแบ่งปิดกำไรอัตโนมัติแบบ 3 ส่วนตามแผนของกลยุทธ์ที่เปิดไม้นั้น (เช่น `BALANCED` แบ่งปิด **40% / 30% / 30%**, `TREND_FOLLOW` **25% / 35% / 40%**, `SCALP_QUICK` **50% / 35% / 15%**):
     - **TP1 ชนเป้า**: ปิดตามสัดส่วน TP1 + เลื่อน Stop Loss มา **กันทุน (Breakeven + ค่าธรรมเนียม)** อัตโนมัติ
     - **TP2 ชนเป้า**: ปิดตามสัดส่วน TP2 + เลื่อน **Trailing SL มาล็อกกำไรที่ราคา TP1**
     - **TP3 ชนเป้า**: ปิดส่วนที่เหลือสุดท้าย และบันทึกผลกำไร/Win Rate ทั้งพอร์ตและแยกตามกลยุทธ์
   - บันทึกสถานะพอร์ตลงไฟล์ `paper_state.json` และบันทึกประวัติทุก Action (`OPEN`, `TP1_PARTIAL`, `TP2_PARTIAL`, `TP3_FULL_CLOSE`, `BREAKEVEN_SL`, `TRAILING_SL_TP1`, `MANUAL_CLOSE`, `STOP_LOSS`) พร้อมคอลัมน์ `strategy` ลงไฟล์ `paper_trades.csv` และแจ้งเตือนเข้า Telegram/Discord (`PAPER_NOTIFY_WEBHOOKS=true`)

8. **หน้าจอพอร์ตจำลอง + ปุ่มสั่งการบน Telegram (Interactive Telegram Portfolio Dashboard)**
   - แสดงการ์ดสรุปพอร์ตจำลองแบบเรียลไทม์ (`กลยุทธ์ที่ใช้งาน`, `Equity`, `เงินว่าง`, `Margin ถืออยู่`, `uPnL`, `Realized PnL`, `Win Rate`, `Max DD`, ค่าตั้งไม้ปัจจุบัน และรายการออเดอร์ที่ถืออยู่พร้อมป้าย `[Strategy]`) พร้อมปุ่ม Inline กดสั่งงานจากแชท Telegram ได้ทันที (`PAPER_TELEGRAM_BOT_ENABLED=true`):
     - `🎯 กลยุทธ์` — เลือกสลับ 5 กลยุทธ์แบบปุ่มกด (ปรับเกณฑ์ Min⭐ ของสายนั้นให้อัตโนมัติ โดยคง `Leverage / Size % / Max ไม้` เดิมไว้) พร้อมแสดงตาราง **Win Rate / PnL แยกตามกลยุทธ์**
     - `🔄 รีเฟรชพอร์ต` — ดึงราคาตลาดล่าสุดมาอัปเดต uPnL และแก้ไขการ์ดในข้อความเดิมทันที
     - `📜 ประวัติ 10 รายการ` — ดูประวัติเหตุการณ์เปิด/ปิด/TP/SL 10 รายการล่าสุดจาก CSV (พร้อมป้าย `[Strategy]`)
     - `⏸️ พักเปิดไม้ใหม่` / `▶️ เปิดรับไม้ใหม่` — หยุดเปิดออเดอร์ใหม่ชั่วคราว (ออเดอร์ที่เปิดค้างไว้ยังรัน TP/SL ต่อ)
     - `🔍 สแกนทันที` — สั่งปลุกสแกนเนอร์ให้สแกนรอบใหม่/อัปเดตราคาทันทีโดยไม่ต้องรอครบรอบ
     - `📋 ปิดออเดอร์รายตัว` / `❌ ปิดทุกไม้ทันที` — ปิดออเดอร์ด้วยตนเองที่ราคาตลาดปัจจุบัน (มีหน้ายืนยัน 2 ขั้นสำหรับปิดทั้งหมด)
     - `⚙️ ตั้งค่าพอร์ต` — ปรับ **จำนวนไม้สูงสุด (`3–15 ไม้`), ขนาดต่อไม้ (`5–25%`), Leverage (`3–20x`), และเกณฑ์ดาว (`≥ 2⭐` / `เฉพาะ 3⭐`)** แบบปุ่มกดทันที พร้อมระบบคำนวณคำเตือนความเสี่ยงอัตโนมัติ (เมื่อ Leverage `>= 20x` หรือ `Max × Size > 100%`) และจดจำค่าถาวรใน `paper_state.json` (คงค่าตั้งไว้แม้รีสตาร์ทหรือรีเซ็ตพอร์ต จนกว่าจะกด `🔄 คืนค่าเริ่มต้น (.env)`)
     - `🔄 รีเซ็ตพอร์ต` — ล้างประวัติและตั้งต้นทุนจำลองใหม่ (มีหน้ายืนยัน 2 ขั้นก่อนล้างข้อมูล — ล้างเฉพาะสถิติแพ้/ชนะและสถิติแยกกลยุทธ์ โดยคงกลยุทธ์และค่าตั้งพอร์ตที่ปรับไว้)
   - การแจ้งเตือนทุกเหตุการณ์ของ Paper Trading จะแนบปุ่มลัด `💼 ดูพอร์ต + ปุ่มสั่งการ` และ `📜 ประวัติ 10 รายการ` มาด้วยเสมอ
   - ป้องกันความปลอดภัยโดยอนุญาตเฉพาะแชทที่ตรงกับ `TELEGRAM_CHAT_ID` เท่านั้น และเคลียร์คำสั่งค้างเก่าอัตโนมัติตอนเริ่มรัน

9. **ระบบแจ้งเตือนพร้อมป้องกันสแปม (Telegram & Discord Webhook + Cooldown & State Cache)**
   - รองรับทั้ง **Telegram Bot API** (รูปแบบ HTML พร้อมลิงก์เปิดกราฟ Binance Futures และ Interactive Bot Polling) และ **Discord Webhook** (Rich Embed แยกสีเขียว/แดงตามทิศทางสัญญาณ)
   - มีระบบ **Cooldown ต่อเหรียญและประเภทสัญญาณ** (`ALERT_COOLDOWN_SECS`) และบันทึกสถานะลง `.scanner_state.json` เพื่อจดจำค่า Open Interest และ Cooldown ข้ามรอบการทำงาน

---

## 🧪 คำสั่งใช้งานระบบจำลองการเทรด & บอท Telegram (Paper Trading)

สามารถใช้ได้ทั้งเวอร์ชัน Rust และเวอร์ชันมือถือ (`mobile_scanner.py`):

| คำสั่ง (Mobile Python) | คำสั่ง (Rust) | หน้าที่ |
| :--- | :--- | :--- |
| `python3 mobile_scanner.py` | `cargo run --release` | สแกนตลาด + จำลองเทรด + เปิดบอท Telegram รอรับปุ่มคำสั่ง |
| `python3 mobile_scanner.py --paper-status` | `cargo run --release -- --paper-status` | ดูสรุปพอร์ตจำลอง Equity, Win Rate, Max DD และไม้ที่ถืออยู่ |
| `python3 mobile_scanner.py --paper-status --send` | `cargo run --release -- --paper-status --send` | ดูพอร์ตจำลองบนเทอร์มินัล + ส่งการ์ดพอร์ตพร้อมปุ่มสั่งการเข้า Telegram |
| `python3 mobile_scanner.py --paper-reset` | `cargo run --release -- --paper-reset` | รีเซ็ตพอร์ตจำลองกลับเป็น `$1,000 USDT` และล้าง CSV เก่า |
| `python3 mobile_scanner.py --paper-self-test` | `cargo test` | ทดสอบความถูกต้องของกลยุทธ์ TP1/TP2/TP3/BE/SL และหน้าจอพอร์ต |

### คำสั่งแชทใน Telegram (Slash Commands)
พิมพ์ส่งในแชท Telegram ที่มี `TELEGRAM_CHAT_ID` ตรงกันได้ทันที (บอทจะลงทะเบียนเมนู `/` ให้อัตโนมัติตอนเริ่มทำงาน):
- `/portfolio` (หรือ `/status`, `/positions`) — ส่งการ์ดสรุปพอร์ตจำลองล่าสุด + ปุ่มสั่งการ
- `/strategy` (หรือ `/strat`, `/mode`) — เปิดหน้าเลือกกลยุทธ์ 5 แบบ + สถิติ Win Rate/PnL แยกสาย หรือสลับทันที เช่น `/strategy trend`, `/strategy scalp`
- `/settings` (หรือ `/config`) — เปิดหน้าตั้งค่าพอร์ตแบบปุ่มกด (`Max ไม้`, `Size %`, `Leverage`, `ระดับดาว`, `คืนค่าเริ่มต้น .env`)
- `/set` — พิมพ์กำหนดค่าละเอียดด้วยตัวเอง เช่น `/set strategy trend lev 15`, `/set max 8 size 12.5 stars 3`, หรือ `/set default`
- `/history` — ดูประวัติการเปิด/ปิด/TP/SL 10 รายการล่าสุด
- `/scan` — สแกนตลาด / อัปเดตราคาทันที
- `/pause` — พักการเปิดออเดอร์ใหม่ชั่วคราว
- `/resume` — เปิดรับออเดอร์จำลองใหม่อีกครั้ง
- `/closeall` — เปิดหน้ายืนยันเพื่อปิดออเดอร์จำลองทั้งหมดทันที
- `/reset` — เปิดหน้ายืนยันเพื่อรีเซ็ตพอร์ตจำลองกลับเป็นทุนเริ่มต้น
- `/help` — แสดงรายการคำสั่งทั้งหมดของบอท

### ตัวแปรตั้งค่าใน `.env` สำหรับกลยุทธ์, Paper Trading & Telegram Bot
| ตัวแปร | ค่าเริ่มต้น | คำอธิบาย |
| :--- | :---: | :--- |
| `SCANNER_STRATEGY` | `BALANCED` | กลยุทธ์เริ่มต้น (`BALANCED`, `TREND_FOLLOW`, `BREAKOUT`, `REVERSAL_SNIPER`, `SCALP_QUICK`) |
| `PAPER_TRADING_ENABLED` | `true` | เปิด/ปิด ระบบจำลองการเทรดอัตโนมัติ |
| `PAPER_INITIAL_BALANCE` | `1000.0` | เงินทุนจำลองเริ่มต้น (USDT) |
| `PAPER_LEVERAGE` | `10.0` | ตัวคูณ Leverage จำลอง |
| `PAPER_POSITION_PCT` | `10.0` | ขนาด Margin ต่อไม้ (% ของ Equity ปัจจุบัน) |
| `PAPER_MAX_OPEN_POSITIONS` | `5` | จำนวนออเดอร์ที่เปิดพร้อมกันได้สูงสุด |
| `PAPER_MIN_STARS` | `2` | ระดับดาวขั้นต่ำของ `BALANCED` (`2` = 2–3 ดาว, `3` = เฉพาะ 3 ดาว) |
| `PAPER_TAKER_FEE_PCT` | `0.05` | ค่าธรรมเนียม Taker จำลองต่อข้าง (%) |
| `PAPER_NOTIFY_WEBHOOKS` | `true` | ส่งแจ้งเตือนเปิด/ปิด/TP/SL ของพอร์ตจำลองเข้า Telegram/Discord |
| `PAPER_TELEGRAM_BOT_ENABLED` | `true` | เปิด/ปิด ระบบแสดงพอร์ตจำลอง + ปุ่มสั่งการผ่าน Telegram |
| `PAPER_TELEGRAM_SUMMARY_MINS` | `0` | ส่งสรุปพอร์ตจำลองเข้า Telegram อัตโนมัติทุก N นาที (`0` = ปิด) |
| `PAPER_STATE_FILE` | `paper_state.json` | ไฟล์เก็บสถานะพอร์ตจำลองข้ามรอบการรัน |
| `PAPER_TRADES_CSV` | `paper_trades.csv` | ไฟล์ประวัติ Log การเทรดทุกเหตุการณ์ (20 คอลัมน์ รวม `strategy`) |

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
| `TELEGRAM_CHAT_ID` | Chat ID หรือ Channel/Group ID ที่ต้องการให้บอทส่งแจ้งเตือนและอนุญาตสั่งการ |
| `DISCORD_WEBHOOK_URL` | Webhook URL ของห้อง Discord ที่ต้องการรับแจ้งเตือน |
| `HTTPS_PROXY` *(ทางเลือก)* | กรณีใช้ HTTP/HTTPS Proxy แทน Reverse Proxy |

### ขั้นตอนที่ 3: กดรันจากแท็บ Actions บน GitHub
1. ไปที่แท็บ **Actions** ของ Repository บน GitHub
2. เลือกเวิร์กโฟลว์ **Binance Futures Scanner (Scanner-f)** ด้านซ้ายมือ
3. กดปุ่ม **Run workflow** แล้วเลือกโหมดที่ต้องการ:
   - `test-alert`: ทดสอบส่งข้อความแจ้งเตือน + การ์ดพอร์ตจำลองเข้า Telegram / Discord ทันที
   - `once`: สแกนทุกเหรียญในตลาด 1 รอบ แล้วส่งการ์ดสรุปพอร์ตจำลองก่อนจบการทำงาน
   - `realtime`: เปิดเชื่อมต่อ WebSocket สแกนแบบเรียลไทม์ต่อเนื่อง + บอทปุ่มสั่งการ Telegram ตามจำนวนนาทีที่กำหนด (`duration_mins`, สูงสุด 350 นาทีต่อรอบ)

---

## 📱 วิธีรันในมือถือแบบง่ายที่สุด (ไม่ติดบล็อก IP 100%)

เนื่องจากอินเทอร์เน็ตมือถือในไทยสามารถเชื่อมต่อ Binance Futures (`fapi.binance.com`) ได้โดยตรงแบบไม่ติดบล็อก คุณสามารถรันสแกนเนอร์ผ่านมือถือได้ทันที:

### สำหรับ Android (ผ่านแอป Termux) — ก๊อปปี้คำสั่งเดียวรันได้เลย
เปิดแอป **Termux** แล้ววางคำสั่งนี้:
```bash
curl -sL https://raw.githubusercontent.com/purinus0341-ai/Scanner-f/main/termux-run.sh | bash
```
- สคริปต์จะถามค่า `Telegram Bot Token` / `Chat ID` / `Discord Webhook` (หรือกด Enter ข้ามเพื่อดูผลแจ้งเตือนสดบนหน้าจอมือถือได้เลย)
- เปิด `termux-wake-lock` ให้อัตโนมัติเพื่อให้สแกนต่อได้แม้ปิดหน้าจอมือถือ
- มีเมนูให้เลือกครบทั้ง **โหมดรันทันที + จำลองเทรด + บอทสั่งการ Telegram (`mobile_scanner.py`)**, **ดูพอร์ตจำลอง + ส่งเข้า Telegram (`--paper-status --send`)**, **รีเซ็ตพอร์ต (`--paper-reset`)**, และ **โหมด Rust เต็มรูปแบบ (`cargo build --release -j 1` อัตโนมัติบน Termux เพื่อเลี่ยงบั๊ก Android `Text file busy`)**

### สำหรับ iPhone / iPad (ผ่านแอป a-Shell หรือ Pythonista) หรือแอป Python บนมือถือ
ดาวน์โหลดและรันไฟล์ [`mobile_scanner.py`](mobile_scanner.py) ได้ทันทีโดยไม่ต้องติดตั้งไลบรารีเสริม (`pip`):
```bash
curl -sLO https://raw.githubusercontent.com/purinus0341-ai/Scanner-f/main/mobile_scanner.py
python3 mobile_scanner.py
```

---

## 💻 วิธีรันบนเครื่องหรือเซิร์ฟเวอร์ส่วนตัว (Local / VPS)

```bash
# 1. คัดลอกไฟล์ตั้งค่า
cp .env.example .env

# 2. ทดสอบส่งแจ้งเตือน + การ์ดพอร์ตจำลองบน Telegram
cargo run --release -- --test-alert

# 3. สแกนตลาดแบบรอบเดียว + จำลองการเทรด
cargo run --release -- --once

# 4. ดูสรุปพอร์ตจำลอง (+ ส่งเข้า Telegram) / รีเซ็ตพอร์ต
cargo run --release -- --paper-status --send
cargo run --release -- --paper-reset

# 5. รันสแกนแบบเรียลไทม์ 24/7 + บอท Telegram รอรับปุ่มสั่งการ
cargo run --release
```

