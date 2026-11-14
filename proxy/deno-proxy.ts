/**
 * Reverse Proxy สำหรับ Deno Deploy / Deno Server นอกสหรัฐฯ
 * รองรับทั้ง REST (/fapi/*) และ WebSocket (/stream*, /ws*)
 *
 * รันทดสอบในเครื่อง:
 *   deno run --allow-net proxy/deno-proxy.ts
 */

const REST_UPSTREAM = "https://fapi.binance.com";
const WS_UPSTREAM = "wss://fstream.binance.com";

Deno.serve(async (req: Request) => {
  const url = new URL(req.url);

  if (url.pathname === "/health") {
    return Response.json({ ok: true, service: "scanner-f-deno-proxy" });
  }

  // จัดการ WebSocket Upgrade (/stream, /ws)
  if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
    const upstreamUrl = `${WS_UPSTREAM}${url.pathname}${url.search}`;
    const { socket: clientWs, response } = Deno.upgradeWebSocket(req);
    const upstreamWs = new WebSocket(upstreamUrl);

    upstreamWs.onmessage = (e) => {
      if (clientWs.readyState === WebSocket.OPEN) {
        clientWs.send(e.data);
      }
    };
    upstreamWs.onclose = (e) => {
      if (clientWs.readyState === WebSocket.OPEN) {
        clientWs.close(e.code || 1000, e.reason);
      }
    };
    upstreamWs.onerror = () => {
      if (clientWs.readyState === WebSocket.OPEN) {
        clientWs.close(1011, "Upstream WebSocket error");
      }
    };

    clientWs.onmessage = (e) => {
      if (upstreamWs.readyState === WebSocket.OPEN) {
        upstreamWs.send(e.data);
      }
    };
    clientWs.onclose = () => {
      if (upstreamWs.readyState === WebSocket.OPEN) {
        upstreamWs.close();
      }
    };

    return response;
  }

  // จัดการ REST API (/fapi/*)
  const targetUrl = `${REST_UPSTREAM}${url.pathname}${url.search}`;
  const headers = new Headers(req.headers);
  headers.set("host", "fapi.binance.com");
  headers.delete("x-forwarded-for");
  headers.delete("x-real-ip");

  return fetch(targetUrl, {
    method: req.method,
    headers,
    body: req.method !== "GET" && req.method !== "HEAD" ? req.body : undefined,
  });
});
