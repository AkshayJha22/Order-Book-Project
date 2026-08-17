import type { WebSocket } from "ws";
import type { Metrics } from "./metrics.js";

interface ClientState {
  /** The newest frame not yet written. Overwriting this IS the conflation. */
  pending: string | null;
  /** Consecutive flushes skipped because the socket was above high water. */
  consecutiveSkips: number;
  sent: number;
  conflated: number;
  connectedAt: number;
}

export interface HubOptions {
  /** Broadcast rate to browsers, Hz. */
  flushHz: number;
  /** Bytes of unflushed socket buffer above which we consider a client behind. */
  highWaterBytes: number;
  /** Consecutive skipped flushes before we give up and disconnect a client. */
  maxConsecutiveSkips: number;
  metrics: Metrics;
}

/**
 * Fan-out with conflation.
 *
 * The naive approach — `for (const c of clients) c.send(frame)` — has an
 * unbounded queue hiding in it. `ws.send()` returns immediately whether or not
 * the bytes went anywhere; if the client's TCP window is full, they pile up in
 * the socket's internal buffer. One laptop on hotel wifi, and the server's
 * memory grows without limit until it dies. Nothing in the naive code looks
 * wrong, and nothing warns you.
 *
 * The fix has two halves:
 *
 *  1. Conflation. Each client gets a single-slot mailbox, not a queue. A new
 *     frame overwrites whatever was waiting. A client that can absorb 2 frames
 *     per second gets the 2 newest frames per second, not the 2 oldest. For a
 *     display client this is strictly better: nobody wants to watch a stale
 *     book replay in slow motion.
 *
 *  2. Backpressure. Before writing we check `bufferedAmount`. Above the high
 *     water mark, we skip this flush entirely — the pending slot stays put and
 *     gets overwritten by something newer. A client that stays behind for
 *     `maxConsecutiveSkips` flushes is disconnected rather than carried
 *     forever.
 *
 * This is only sound because the frame is a self-contained snapshot. If we were
 * shipping deltas, dropping a frame would silently corrupt every client that
 * missed it. The wire format and the backpressure strategy are one decision,
 * not two.
 */
export class Hub {
  private clients = new Map<WebSocket, ClientState>();
  private timer: NodeJS.Timeout | null = null;

  constructor(private readonly opts: HubOptions) {}

  start(): void {
    const interval = Math.max(1, Math.round(1000 / this.opts.flushHz));
    this.timer = setInterval(() => this.flush(), interval);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
  }

  get size(): number {
    return this.clients.size;
  }

  add(ws: WebSocket): void {
    this.clients.set(ws, {
      pending: null,
      consecutiveSkips: 0,
      sent: 0,
      conflated: 0,
      connectedAt: Date.now(),
    });
  }

  remove(ws: WebSocket): void {
    this.clients.delete(ws);
  }

  /** Send one frame immediately to one client, bypassing conflation. */
  sendDirect(ws: WebSocket, frame: string): void {
    if (ws.readyState !== ws.OPEN) return;
    ws.send(frame);
  }

  /** Queue the newest state for every client. Older pending frames are dropped. */
  publish(frame: string): void {
    this.opts.metrics.framesPublished++;
    for (const state of this.clients.values()) {
      if (state.pending !== null) {
        state.conflated++;
        this.opts.metrics.framesConflated++;
      }
      state.pending = frame;
    }
  }

  private flush(): void {
    for (const [ws, state] of this.clients) {
      if (state.pending === null) continue;

      if (ws.readyState !== ws.OPEN) {
        this.clients.delete(ws);
        continue;
      }

      if (ws.bufferedAmount > this.opts.highWaterBytes) {
        state.consecutiveSkips++;
        this.opts.metrics.slowConsumerSkips++;
        if (state.consecutiveSkips >= this.opts.maxConsecutiveSkips) {
          // This client has been unable to keep up for a sustained window.
          // Holding the slot open costs memory and helps nobody, so close it
          // with a code the browser can distinguish from a network failure.
          this.opts.metrics.slowConsumerDrops++;
          this.clients.delete(ws);
          ws.close(1013, "client too slow");
        }
        continue;
      }

      state.consecutiveSkips = 0;
      ws.send(state.pending);
      state.pending = null;
      state.sent++;
      this.opts.metrics.framesSent++;
    }
  }

  /** Per-client stats, for the /metrics endpoint. */
  clientStats() {
    const now = Date.now();
    return [...this.clients.entries()].map(([ws, s]) => ({
      ageSec: Math.round((now - s.connectedAt) / 1000),
      sent: s.sent,
      conflated: s.conflated,
      bufferedBytes: ws.bufferedAmount,
      skipping: s.consecutiveSkips > 0,
    }));
  }
}
