import { EventEmitter } from "node:events";
import WebSocket from "ws";
import { BookSynchronizer } from "./sync.js";
import type { DepthEvent, DepthSnapshot, FeedState } from "./types.js";
import type { Metrics } from "./metrics.js";
import type { OrderBook } from "./orderbook.js";

/** Common surface for any feed the server can be pointed at. */
export interface Feed extends EventEmitter {
  readonly book: OrderBook;
  readonly currentState: FeedState;
  start(): void;
  stop(): void;
}

export interface BinanceFeedOptions {
  symbol: string;
  /** "100ms" for the throttled stream, "realtime" for the raw one. */
  speed: "100ms" | "realtime";
  snapshotDepth: number;
  metrics: Metrics;
}

/**
 * Transport for Binance's diff-depth stream. Owns the socket lifecycle,
 * reconnection, and the staleness watchdog. All correctness logic lives in
 * BookSynchronizer, which this class simply feeds.
 */
export class BinanceFeed extends EventEmitter implements Feed {
  private readonly sync: BookSynchronizer;
  private ws: WebSocket | null = null;
  private state: FeedState = "connecting";

  private reconnectAttempt = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private watchdog: NodeJS.Timeout | null = null;
  private lastMessageAt = 0;
  private closed = false;

  private static readonly WATCHDOG_MS = 10_000;
  private static readonly BACKOFF_BASE_MS = 250;
  private static readonly BACKOFF_CAP_MS = 15_000;

  constructor(private readonly opts: BinanceFeedOptions) {
    super();
    this.sync = new BookSynchronizer(() => this.fetchSnapshot(), opts.metrics);
    this.sync.on("state", (s: FeedState, detail?: string) => this.setState(s, detail));
    this.sync.on("update", (ts: number) => this.emit("update", ts));
    this.sync.on("error", (e) => this.emit("feed-error", e));
  }

  get book(): OrderBook {
    return this.sync.book;
  }

  get currentState(): FeedState {
    return this.state;
  }

  start(): void {
    this.closed = false;
    this.connect();
    this.watchdog = setInterval(() => this.checkStaleness(), 2000);
  }

  stop(): void {
    this.closed = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.watchdog) clearInterval(this.watchdog);
    this.sync.stop();
    this.ws?.removeAllListeners();
    this.ws?.close();
    this.ws = null;
  }

  private setState(state: FeedState, detail?: string): void {
    this.state = state;
    this.emit("state", state, detail);
  }

  private async fetchSnapshot(): Promise<DepthSnapshot> {
    const url =
      `https://api.binance.com/api/v3/depth` +
      `?symbol=${this.opts.symbol.toUpperCase()}&limit=${this.opts.snapshotDepth}`;
    const res = await fetch(url, { signal: AbortSignal.timeout(10_000) });
    if (!res.ok) throw new Error(`snapshot HTTP ${res.status}`);
    return (await res.json()) as DepthSnapshot;
  }

  private connect(): void {
    if (this.closed) return;

    const suffix = this.opts.speed === "100ms" ? "@depth@100ms" : "@depth";
    const url = `wss://stream.binance.com:9443/ws/${this.opts.symbol.toLowerCase()}${suffix}`;

    this.setState("connecting", url);
    const ws = new WebSocket(url);
    this.ws = ws;

    ws.on("open", () => {
      this.reconnectAttempt = 0;
      this.lastMessageAt = Date.now();
      // Buffering starts now, and the snapshot fetch races the stream. That
      // race is not a bug to be avoided — it is precisely what the
      // reconciliation algorithm exists to resolve.
      this.sync.begin("stream open, fetching snapshot");
    });

    ws.on("message", (raw: WebSocket.RawData) => {
      this.lastMessageAt = Date.now();
      try {
        this.sync.push(JSON.parse(raw.toString()) as DepthEvent);
      } catch {
        /* malformed frame: ignore */
      }
    });

    ws.on("error", (err) => this.emit("feed-error", err));

    ws.on("close", (code, reason) => {
      if (this.closed) return;
      this.sync.stop();
      this.setState("disconnected", `code=${code} ${reason.toString()}`);
      this.scheduleReconnect();
    });
  }

  private checkStaleness(): void {
    if (this.closed || !this.ws) return;
    if (this.ws.readyState !== WebSocket.OPEN) return;
    if (Date.now() - this.lastMessageAt > BinanceFeed.WATCHDOG_MS) {
      // A TCP connection can stay "open" long after it has stopped delivering
      // anything. Silence is a failure mode, so treat it as one rather than
      // waiting for a FIN that may never arrive.
      this.emit("feed-error", new Error("upstream silent, forcing reconnect"));
      this.ws.terminate();
    }
  }

  private scheduleReconnect(): void {
    if (this.closed) return;
    this.opts.metrics.reconnects++;
    // Full jitter. Without it, every client that dropped during an exchange
    // blip reconnects in lockstep and stampedes the endpoint together.
    const ceiling = Math.min(
      BinanceFeed.BACKOFF_CAP_MS,
      BinanceFeed.BACKOFF_BASE_MS * 2 ** this.reconnectAttempt,
    );
    const delay = Math.random() * ceiling;
    this.reconnectAttempt = Math.min(this.reconnectAttempt + 1, 12);
    this.setState("disconnected", `reconnecting in ${Math.round(delay)}ms`);
    this.reconnectTimer = setTimeout(() => this.connect(), delay);
  }
}
