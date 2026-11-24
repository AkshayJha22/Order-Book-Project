import { EventEmitter } from "node:events";
import { OrderBook } from "./orderbook.js";
import type { DepthEvent, DepthSnapshot, FeedState } from "./types.js";
import type { Metrics } from "./metrics.js";

/**
 * Reconciles a REST snapshot with a live delta stream into a correct local
 * replica of an order book. Deliberately knows nothing about websockets, HTTP,
 * or which exchange it is talking to — it takes events in and a snapshot-fetch
 * callback, and that is all.
 *
 * That separation is what lets the synthetic feed (synthetic.ts) drive this
 * exact code path. Testing your sync logic against a fake transport that can
 * inject gaps on demand is far more convincing than hoping the real exchange
 * drops a packet while you happen to be watching.
 *
 * The reconciliation procedure:
 *
 *   1. Open the stream and buffer every event; apply nothing yet.
 *   2. Fetch the snapshot; note its `lastUpdateId`.
 *   3. Discard buffered events whose final ID `u` is below the snapshot — they
 *      are already baked in.
 *   4. The first event applied must *straddle* the snapshot:
 *      `U <= lastUpdateId + 1 <= u`. If nothing straddles it and events have
 *      run ahead, there is a hole; refetch.
 *   5. Thereafter every event must satisfy `U == previous_u + 1`. Anything else
 *      means a lost or reordered message, and the replica is now wrong.
 *
 * Step 5 is the one that gets skipped. Skipping it doesn't throw — it leaves a
 * stale level sitting there looking perfectly plausible until the book crosses.
 */
export class BookSynchronizer extends EventEmitter {
  readonly book = new OrderBook();

  private buffer: DepthEvent[] = [];
  private awaitingFirst = true;
  private draining = false;
  private stopped = false;
  private snapshotTimer: NodeJS.Timeout | null = null;

  private static readonly MAX_BUFFER = 20_000;

  constructor(
    private readonly fetchSnapshot: () => Promise<DepthSnapshot>,
    private readonly metrics: Metrics,
  ) {
    super();
  }

  /** Reset all state and begin a fresh buffer-then-snapshot cycle. */
  begin(detail = "buffering, fetching snapshot"): void {
    this.stopped = false;
    this.buffer = [];
    this.awaitingFirst = true;
    this.draining = false;
    this.book.clear();
    this.emit("state", "buffering" satisfies FeedState, detail);
    void this.loadSnapshot();
  }

  stop(): void {
    this.stopped = true;
    if (this.snapshotTimer) clearTimeout(this.snapshotTimer);
    this.buffer = [];
    this.draining = false;
  }

  /** Feed one depth event in from whatever transport is driving this. */
  push(evt: DepthEvent): void {
    if (this.stopped) return;
    if (typeof evt.u !== "number" || typeof evt.U !== "number") return;

    this.metrics.eventsReceived++;

    if (!this.draining) {
      this.buffer.push(evt);
      if (this.buffer.length > BookSynchronizer.MAX_BUFFER) {
        // Bound the buffer if a snapshot fetch hangs. Dropping the oldest is
        // safe here: it only costs us the ability to straddle, which forces a
        // resync rather than corrupting anything.
        this.buffer.splice(0, this.buffer.length - BookSynchronizer.MAX_BUFFER);
      }
      return;
    }
    this.ingest(evt);
  }

  private async loadSnapshot(): Promise<void> {
    if (this.stopped) return;
    try {
      const snap = await this.fetchSnapshot();
      if (this.stopped) return;
      if (typeof snap.lastUpdateId !== "number") throw new Error("malformed snapshot");

      this.book.applySnapshot(snap.bids, snap.asks, snap.lastUpdateId);
      this.awaitingFirst = true;
      this.replayBuffer();
    } catch (err) {
      this.emit("error", err);
      // Retry without tearing down the transport. The buffer keeps filling, so
      // a later retry can still reconcile cleanly.
      if (!this.stopped) {
        this.snapshotTimer = setTimeout(() => void this.loadSnapshot(), 1000);
      }
    }
  }

  private replayBuffer(): void {
    const pending = this.buffer;
    this.buffer = [];
    for (const evt of pending) {
      if (!this.ingest(evt)) return; // resync fired mid-replay; it owns the state now
    }
    this.draining = true;
    if (!this.awaitingFirst) {
      this.emit("state", "live" satisfies FeedState);
    } else {
      // Snapshot is newer than everything buffered. Legal — the next live event
      // should be the one that straddles it.
      this.emit(
        "state",
        "buffering" satisfies FeedState,
        "snapshot ahead of buffer, awaiting straddle event",
      );
    }
  }

  /** Returns false if a resync fired and the caller must stop iterating. */
  private ingest(evt: DepthEvent): boolean {
    const t0 = performance.now();
    const expected = this.book.lastUpdateId + 1;

    if (evt.u < expected) {
      this.metrics.eventsStale++;
      return true;
    }

    if (this.awaitingFirst) {
      const straddles = evt.U <= expected && evt.u >= expected;
      if (!straddles) {
        this.metrics.gapsDetected++;
        this.resync(`no straddle: snapshot=${this.book.lastUpdateId}, evt.U=${evt.U}`);
        return false;
      }
      this.awaitingFirst = false;
      this.emit("state", "live" satisfies FeedState);
    } else if (evt.U !== expected) {
      this.metrics.gapsDetected++;
      this.resync(`sequence gap: expected U=${expected}, got U=${evt.U}`);
      return false;
    }

    this.book.applyDelta(evt.b ?? [], evt.a ?? [], evt.u);

    if (this.book.isCrossed()) {
      this.metrics.crossedBooks++;
      this.resync(`crossed book: bid=${this.book.bestBid} >= ask=${this.book.bestAsk}`);
      return false;
    }

    this.metrics.applyLatency.record(performance.now() - t0);
    this.emit("update", evt.E ?? Date.now());
    return true;
  }

  private resync(reason: string): void {
    this.metrics.resyncs++;
    this.emit("state", "resyncing" satisfies FeedState, reason);
    this.buffer = [];
    this.draining = false;
    this.awaitingFirst = true;
    this.book.clear();
    void this.loadSnapshot();
  }
}
