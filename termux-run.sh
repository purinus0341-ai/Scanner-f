#!/usr/bin/env bash
set -e

REPO_URL="https://github.com/purinus0341-ai/Scanner-f.git"
INSTALL_DIR="$HOME/Scanner-f"

echo "=============================================================="
echo "📱 Scanner-f : สคริปต์ติดตั้งและรันบนมือถือ (Termux / Linux)"
echo "=============================================================="

# 1. ตรวจสอบสภาพแวดล้อม Termux
if command -v pkg >/dev/null 2>&1; then
    echo "🔹 ตรวจพบระบบ Termux (Android)"
    if ! command -v git >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
        echo "📦 กำลังติดตั้ง git และ python (ใช้เวลาไม่กี่วินาที)..."
        pkg update -y
        pkg install -y git python
    fi

    # เปิด Wake Lock เพื่อไม่ให้มือถือตัดการทำงานเมื่อปิดหน้าจอ
    if command -v termux-wake-lock >/dev/null 2>&1; then
        termux-wake-lock || true
        echo "🔒 เปิดใช้งาน termux-wake-lock แล้ว (สแกนต่อได้แม้ปิดหน้าจอมือถือ)"
    fi
fi

# 2. ดาวน์โหลดหรืออัปเดตโปรเจกต์
if [ -f "Cargo.toml" ] && grep -q 'name = "scanner-f"' Cargo.toml 2>/dev/null; then
    INSTALL_DIR="$(pwd)"
else
    if [ ! -d "$INSTALL_DIR/.git" ]; then
        echo "⬇️ กำลังดาวน์โหลด Scanner-f ไปยัง $INSTALL_DIR ..."
        git clone "$REPO_URL" "$INSTALL_DIR"
    else
        echo "🔄 กำลังอัปเดตโค้ดล่าสุดจาก GitHub..."
        git -C "$INSTALL_DIR" pull --ff-only || true
    fi
    cd "$INSTALL_DIR"
fi

# 3. ฟังก์ชันตั้งค่า .env แบบถาม-ตอบบนมือถือ
setup_env() {
    echo ""
    echo "--------------------------------------------------------------"
    echo "⚙️ ตั้งค่าการแจ้งเตือน (กด Enter ข้ามได้หากต้องการดูบนหน้าจอมือถืออย่างเดียว)"
    echo "--------------------------------------------------------------"
    read -r -p "👉 Telegram Bot Token (เว้นว่างได้): " TG_TOKEN < /dev/tty || TG_TOKEN=""
    read -r -p "👉 Telegram Chat ID (เว้นว่างได้): " TG_CHAT < /dev/tty || TG_CHAT=""
    read -r -p "👉 Discord Webhook URL (เว้นว่างได้): " DC_WEBHOOK < /dev/tty || DC_WEBHOOK=""
    read -r -p "👉 Timeframe แท่งเทียน [ค่าเริ่มต้น 5m]: " TF_INPUT < /dev/tty || TF_INPUT=""
    TF_INPUT="${TF_INPUT:-5m}"

    cp -f .env.example .env
    sed -i "s|^TELEGRAM_BOT_TOKEN=.*|TELEGRAM_BOT_TOKEN=${TG_TOKEN}|" .env
    sed -i "s|^TELEGRAM_CHAT_ID=.*|TELEGRAM_CHAT_ID=${TG_CHAT}|" .env
    sed -i "s|^DISCORD_WEBHOOK_URL=.*|DISCORD_WEBHOOK_URL=${DC_WEBHOOK}|" .env
    sed -i "s|^KLINE_INTERVAL=.*|KLINE_INTERVAL=${TF_INPUT}|" .env

    echo "✅ บันทึกการตั้งค่าลงไฟล์ .env เรียบร้อย!"
}

if [ ! -f ".env" ]; then
    setup_env
fi

echo ""
echo "=============================================================="
echo "🚀 เลือกโหมดการทำงานบนมือถือ:"
echo "  1) ⚡ โหมดรันทันที (Mobile Fast Mode - ไม่ต้องรอคอมไพล์ ประหยัดแบต)"
echo "  2) 🦀 โหมด Rust เต็มรูปแบบ (WebSocket เรียลไทม์ + ดัก Liquidation สด)"
echo "  3) 🔔 ทดสอบส่งข้อความแจ้งเตือนเข้า Telegram / Discord (--test-alert)"
echo "  4) ⚙️ แก้ไขการตั้งค่า Telegram / Discord / Timeframe ใหม่"
echo "=============================================================="
read -r -p "เลือกหมายเลข [1-4, ค่าเริ่มต้น=1]: " CHOICE < /dev/tty || CHOICE="1"
CHOICE="${CHOICE:-1}"

case "$CHOICE" in
    2|rust)
        if ! command -v cargo >/dev/null 2>&1; then
            if command -v pkg >/dev/null 2>&1; then
                echo "📦 กำลังติดตั้ง Rust สำหรับ Termux..."
                pkg install -y rust clang make pkg-config
            else
                curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
                # shellcheck disable=SC1091
                source "$HOME/.cargo/env"
            fi
        fi
        echo "🔨 กำลังคอมไพล์ Scanner-f (Rust Release)..."
        cargo build --release
        ./target/release/scanner-f
        ;;
    3|test)
        python3 mobile_scanner.py --test-alert
        ;;
    4|config)
        setup_env
        python3 mobile_scanner.py
        ;;
    *)
        python3 mobile_scanner.py
        ;;
esac
