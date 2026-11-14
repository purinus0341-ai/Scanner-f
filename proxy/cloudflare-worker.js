/**
 * Reverse Proxy สำหรับ Binance USDT-M Futures (REST + WebSocket)
 * รองรับการ Deploy บน Cloudflare Workers (เลือกเปิด Smart Placement หาก PoP อยู่ในสหรัฐฯ)
 *
 * วิธีใช้:
 * - ตั้งค่า BINANCE_FAPI_URL = https://<your-worker>.workers.dev
 * - ตั้งค่า BINANCE_FSTREAM_URL = wss://<your-worker>.workers.dev
 */

const REST_UPSTREAM = "https://fapi.binance.com";
const WS_UPSTREAM = "https://fstream.binance.com";

export default {
  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return new Response(JSON.stringify({ ok: true, service: "scanner-f-proxy" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }

    const isWebSocket =
      request.headers.get("Upgrade")?.toLowerCase() === "websocket" ||
      url.pathname.startsWith("/ws") ||
      url.pathname.startsWith("/stream");

    const targetBase = isWebSocket ? WS_UPSTREAM : REST_UPSTREAM;
    const targetUrl = new URL(url.pathname + url.search, targetBase);

    const headers = new Headers(request.headers);
    headers.set("Host", targetUrl.host);
    // ลบ header ที่ระบุ IP ต้นทางจาก US runner ออก
    headers.delete("cf-connecting-ip");
    headers.delete("x-forwarded-for");
    headers.delete("x-real-ip");

    const init = {
      method: request.method,
      headers,
      redirect: "follow",
    };

    if (request.method !== "GET" && request.method !== "HEAD") {
      init.body = request.body;
    }

    return fetch(targetUrl.toString(), init);
  },
};
