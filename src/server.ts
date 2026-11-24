import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer, type WebSocket } from "ws";

import { Metrics } from "./metrics.js";
import { Hub } from "./hub.js";
import { BinanceFeed, type Feed } from "./upstream.js";
import { SyntheticFeed } from "./synthetic.js";
import type { BookMsg, FeedState, HelloMsg, StatusMsg } from "./types.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.resolve(__dirname, "..", "public");

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

function arg(name: string, fallback: string): string {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : (process.env[name.toUpperCase().replace(/-/g, "_")] ?? fallback);
}

const config = {
  port: Number(arg("port", "8080")),
  symbol: arg("symbol", "BTCUSDT"),
  source: arg("source", "binance") as "binance" | "synthetic",
  speed: arg("speed", "100ms") as "100ms" | "realtime",
  /** Levels per side sent to the browser. */
  depth: Number(arg("depth", "20")),
  /** Levels per side requested in the REST snapshot. */
  snapshotDepth: Number(arg("snapshot-depth", "1000")),
  /** Broadcast rate to browsers. */
  flushHz: Number(arg("flush-hz", "20")),
  /** Synthetic feed only. */
  rate: Number(arg("rate", "500")),
  injectGaps: Number(arg("inject-gaps", "0")),
  highWaterBytes: Number(arg("high-water", "262144")),
  maxConsecutiveSkips: Number(arg("max-skips", "200")),
};

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

const metrics = new Metrics();

const hub = new Hub({
  flushHz: config.flushHz,
  highWaterBytes: config.highWaterBytes,
  maxConsecutiveSkips: config.maxConsecutiveSkips,
  metrics,
});

const feed: Feed =
  config.source === "synthetic"
    ? new SyntheticFeed({
        symbol: config.symbol,
        rate: config.rate,
        depth: 200,
        tickSize: 0.01,
        startPrice: 60000,
        injectGaps: config.injectGaps,
        metrics,
      })
    : new BinanceFeed({
        symbol: config.symbol,
        speed: config.speed,
        snapshotDepth: config.snapshotDepth,
        metrics,
      });

let broadcastSeq = 0;
let lastStatus: StatusMsg = {
  type: "status",
  state: "connecting",
  resyncs: 0,
  reconnects: 0,
};

feed.on("state", (state: FeedState, detail?: string) => {
  lastStatus = {
    type: "status",
    state,
    detail,
    resyncs: metrics.resyncs,
    reconnects: metrics.reconnects,
  };
  const frame = JSON.stringify(lastStatus);
  // Status changes are rare and must not be conflated away — a client that
  // misses "resyncing" will happily render a book it thinks is live.
  for (const ws of wss.clients) hub.sendDirect(ws as WebSocket, frame);
  log(`feed: ${state}${detail ? ` — ${detail}` : ""}`);
});

feed.on("feed-error", (err: unknown) => {
  log(`feed error: ${err instanceof Error ? err.message : String(err)}`);
});

/**
 * The book changed. Note we do NOT serialize here — that would mean paying
 * JSON.stringify on every one of potentially 1000 upstream events per second,
 * for a frame most of which will be superseded before anyone sees it. Instead
 * we mark dirty and serialize once per flush tick, below.
 */
let bookDirty = false;
let lastExchangeTs = 0;

feed.on("update", (exchangeTs: number) => {
  bookDirty = true;
  lastExchangeTs = exchangeTs;
});

/** Serialize at the broadcast rate, not the event rate. */
setInterval(() => {
  if (!bookDirty) return;
  bookDirty = false;

  const t0 = performance.now();
  const { bids, asks } = feed.book.top(config.depth);
  const msg: BookMsg = {
    type: "book",
    seq: ++broadcastSeq,
    bids,
    asks,
    lastUpdateId: feed.book.lastUpdateId,
    serverTs: Date.now(),
    exchangeTs: lastExchangeTs,
    depthLevels: feed.book.levelCount,
  };
  const frame = JSON.stringify(msg);
  metrics.serializeLatency.record(performance.now() - t0);
  hub.publish(frame);
}, Math.max(1, Math.round(1000 / config.flushHz)));

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url ?? "/", `http://${req.headers.host}`);

  if (url.pathname === "/metrics") {
    const body = JSON.stringify(
      {
        ...metrics.snapshot(hub.size, feed.currentState),
        config: {
          source: config.source,
          symbol: config.symbol,
          depth: config.depth,
          flushHz: config.flushHz,
          rate: config.source === "synthetic" ? config.rate : undefined,
          injectGaps: config.injectGaps || undefined,
        },
        injectedDrops:
          feed instanceof SyntheticFeed ? feed.droppedEvents : undefined,
        clients: hub.clientStats(),
      },
      null,
      2,
    );
    res.writeHead(200, { "content-type": "application/json" });
    res.end(body);
    return;
  }

  if (url.pathname === "/healthz") {
    const healthy = feed.currentState === "live";
    res.writeHead(healthy ? 200 : 503, { "content-type": "text/plain" });
    res.end(feed.currentState);
    return;
  }

  // Static files. Resolve then verify the result is still inside PUBLIC_DIR,
  // so a path like /../../etc/passwd cannot escape.
  const rel = url.pathname === "/" ? "index.html" : url.pathname.slice(1);
  const filePath = path.resolve(PUBLIC_DIR, rel);
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403).end("forbidden");
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { "content-type": "text/plain" }).end("not found");
      return;
    }
    res.writeHead(200, {
      "content-type": MIME[path.extname(filePath)] ?? "application/octet-stream",
      "cache-control": "no-cache",
    });
    res.end(data);
  });
});

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------

const wss = new WebSocketServer({ server, path: "/stream" });

wss.on("connection", (ws) => {
  hub.add(ws);

  const hello: HelloMsg = {
    type: "hello",
    symbol: config.symbol,
    depth: config.depth,
    source: config.source,
    flushHz: config.flushHz,
  };
  hub.sendDirect(ws, JSON.stringify(hello));
  hub.sendDirect(ws, JSON.stringify(lastStatus));

  // Send current book immediately so a new client isn't staring at an empty
  // screen until the next upstream tick.
  const { bids, asks } = feed.book.top(config.depth);
  if (bids.length || asks.length) {
    const msg: BookMsg = {
      type: "book",
      seq: ++broadcastSeq,
      bids,
      asks,
      lastUpdateId: feed.book.lastUpdateId,
      serverTs: Date.now(),
      exchangeTs: lastExchangeTs,
      depthLevels: feed.book.levelCount,
    };
    hub.sendDirect(ws, JSON.stringify(msg));
  }

  ws.on("close", () => hub.remove(ws));
  ws.on("error", () => hub.remove(ws));
});

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

function log(msg: string): void {
  process.stdout.write(`[${new Date().toISOString()}] ${msg}\n`);
}

hub.start();
feed.start();

server.listen(config.port, () => {
  log(`listening on http://localhost:${config.port}`);
  log(`source=${config.source} symbol=${config.symbol} depth=${config.depth} flushHz=${config.flushHz}`);
  if (config.source === "synthetic") {
    log(`synthetic rate=${config.rate}/s injectGaps=${config.injectGaps}`);
  }
});

let shuttingDown = false;
function shutdown(signal: string): void {
  if (shuttingDown) return;
  shuttingDown = true;
  log(`${signal} received, draining`);
  feed.stop();
  hub.stop();
  for (const ws of wss.clients) ws.close(1001, "server shutting down");
  server.close(() => {
    log("closed");
    process.exit(0);
  });
  // Don't hang forever on a client that refuses to close.
  setTimeout(() => process.exit(0), 3000).unref();
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
