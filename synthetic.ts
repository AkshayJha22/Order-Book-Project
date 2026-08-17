import { EventEmitter } from "node:events";
import { BookSynchronizer } from "./sync.js";
import type { DepthEvent, DepthSnapshot, FeedState } from "./types.js";
import type { Metrics } from "./metrics.js";
import type { OrderBook } from "./orderbook.js";
import type { Feed } from "./upstream.js";

export interface SyntheticFeedOptions {
  symbol: string;
  /** Depth events per second. Binance's throttled stream is 10/s; go higher to
   *  actually exercise conflation and the render loop. */
  rate: number;
  /** Levels maintained per side. */
  depth: number;
  tickSize: number;
  startPrice: number;
  /** Probability that any given event is silently dropped before delivery.
   *  This is the fault injector: a dropped delta must be caught by sequence
   *  checking, not by luck. */
  injectGaps: number;
  metrics: Metrics;
}

/**
 * A local exchange simulator.
 *
 * Two reasons this exists, and neither is "I couldn't reach Binance":
 *
 *  1. Rate. The public throttled stream is 10 events/sec, which never puts the
 *     fan-out or the render loop under any real pressure. Conflation that never
 *     conflates is untested code. Here the rate is a flag.
 *
 *  2. Faults on demand. Correctness of the sync algorithm hinges on detecting a
 *     dropped message, and you cannot ask a real exchange to drop one. With
 *     `injectGaps`, dropped deltas happen on a schedule you control, so
 *     "my resync path works" becomes something you demonstrate rather than
 *     assert.
 *
 * It emits the same event shape as the real feed and drives the same
 * BookSynchronizer, so what gets tested here is the actual production path.
 */
export class SyntheticFeed extends EventEmitter implements Feed {
  private readonly sync: BookSynchronizer;
  private state: FeedState = "connecting";
  private timer: NodeJS.Timeout | null = null;
  private closed = false;

  /** The "exchange truth" book, keyed by integer tick index. */
  private trueBids = new Map<number, number>();
  private trueAsks = new Map<number, number>();
  /** Tick index of the best bid; best ask is always this + 1. */
  private bidTopIdx: number;
  private updateId = 1000;

  /** Faults actually injected, so the README can cite a real number. */
  droppedEvents = 0;

  constructor(private readonly opts: SyntheticFeedOptions) {
    super();
    this.bidTopIdx = Math.round(opts.startPrice / opts.tickSize);
    this.sync = new BookSynchronizer(() => this.snapshot(), opts.metrics);
    this.sync.on("state", (s: FeedState, detail?: string) => this.setState(s, detail));
    this.sync.on("update", (ts: number) => this.emit("update", ts));
    this.sync.on("error", (e) => this.emit("feed-error", e));
    this.seed();
  }

  get book(): OrderBook {
    return this.sync.book;
  }

  get currentState(): FeedState {
    return this.state;
  }

  start(): void {
    this.closed = false;
    this.sync.begin("synthetic feed starting");
    const intervalMs = Math.max(1, Math.round(1000 / this.opts.rate));
    // Generate in batches when the requested rate is faster than the timer can
    // fire. setInterval below ~4ms is unreliable, so we amortize instead of
    // pretending we have microsecond timers.
    const perTick = Math.max(1, Math.round(this.opts.rate / (1000 / intervalMs)));
    this.timer = setInterval(() => {
      for (let i = 0; i < perTick; i++) this.step();
    }, intervalMs);
  }

  stop(): void {
    this.closed = true;
    if (this.timer) clearInterval(this.timer);
    this.sync.stop();
  }

  private setState(state: FeedState, detail?: string): void {
    this.state = state;
    this.emit("state", state, detail);
  }

  private price(idx: number): string {
    return (idx * this.opts.tickSize).toFixed(2);
  }

  private randQty(): number {
    // Depth thins out away from the touch in real books; a lognormal-ish draw
    // is closer to reality than uniform and makes the depth curve look right.
    return +(Math.exp(Math.random() * 2.2) * 0.35).toFixed(4);
  }

  private seed(): void {
    for (let i = 0; i < this.opts.depth; i++) {
      this.trueBids.set(this.bidTopIdx - i, this.randQty());
      this.trueAsks.set(this.bidTopIdx + 1 + i, this.randQty());
    }
  }

  /**
   * The exchange's own view, for tests. The whole point of a simulator is that
   * ground truth is knowable — so correctness can be asserted as "the replica
   * equals the truth", not merely "the replica looks plausible".
   */
  truth(): { bids: Map<number, number>; asks: Map<number, number> } {
    const scale = (m: Map<number, number>) =>
      new Map([...m.entries()].map(([idx, q]) => [+(idx * this.opts.tickSize).toFixed(2), q]));
    return { bids: scale(this.trueBids), asks: scale(this.trueAsks) };
  }

  /**
   * Begin without installing the generator timer, so a caller can drive the
   * feed one event at a time via tick(). Tests that depend on wall-clock timers
   * are flaky tests; this makes the event sequence fully deterministic.
   */
  startManual(): void {
    this.closed = false;
    this.sync.begin("synthetic feed (manually driven)");
  }

  /** Drive exactly one event synchronously. */
  tick(): void {
    this.step();
  }

  /** Serve a snapshot of the truth book, exactly as a REST endpoint would. */
  private async snapshot(): Promise<DepthSnapshot> {
    const bids: [string, string][] = [...this.trueBids.entries()]
      .sort((a, b) => b[0] - a[0])
      .map(([idx, q]) => [this.price(idx), q.toFixed(8)]);
    const asks: [string, string][] = [...this.trueAsks.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([idx, q]) => [this.price(idx), q.toFixed(8)]);
    return { lastUpdateId: this.updateId, bids, asks };
  }

  private step(): void {
    if (this.closed) return;

    const b: [string, string][] = [];
    const a: [string, string][] = [];

    // Occasionally the touch moves. Modelled as the book walking one tick,
    // which consumes a level on one side and creates one on the other — the
    // shape a real trade-through leaves behind.
    if (Math.random() < 0.12) {
      const dir = Math.random() < 0.5 ? 1 : -1;
      this.shift(dir, b, a);
    }

    // Refresh a few levels near the touch. Activity concentrates at the inside
    // in real books, so bias the index toward zero.
    const n = 1 + Math.floor(Math.random() * 3);
    for (let k = 0; k < n; k++) {
      const i = Math.floor(Math.random() ** 2 * this.opts.depth);
      if (Math.random() < 0.5) {
        const idx = this.bidTopIdx - i;
        if (!this.trueBids.has(idx)) continue;
        const q = this.randQty();
        this.trueBids.set(idx, q);
        b.push([this.price(idx), q.toFixed(8)]);
      } else {
        const idx = this.bidTopIdx + 1 + i;
        if (!this.trueAsks.has(idx)) continue;
        const q = this.randQty();
        this.trueAsks.set(idx, q);
        a.push([this.price(idx), q.toFixed(8)]);
      }
    }

    if (b.length === 0 && a.length === 0) return;

    const U = this.updateId + 1;
    const u = U + b.length + a.length - 1;
    this.updateId = u;

    const evt: DepthEvent = { E: Date.now(), U, u, b, a };

    // The fault injector. Note that the truth book has ALREADY been mutated and
    // the update ID has ALREADY advanced — we simply never deliver the event.
    // That is exactly what a lost packet looks like from the client's side, and
    // the only thing standing between it and silent corruption is the
    // `U === lastUpdateId + 1` check in the synchronizer.
    if (this.opts.injectGaps > 0 && Math.random() < this.opts.injectGaps) {
      this.droppedEvents++;
      return;
    }

    this.sync.push(evt);
  }

  /** Walk the inside one tick, keeping the truth book non-crossed. */
  private shift(dir: 1 | -1, b: [string, string][], a: [string, string][]): void {
    const depth = this.opts.depth;

    if (dir === 1) {
      // Price up: the best ask is lifted and becomes the new best bid.
      const lifted = this.bidTopIdx + 1;
      this.trueAsks.delete(lifted);
      a.push([this.price(lifted), "0"]);

      const q = this.randQty();
      this.trueBids.set(lifted, q);
      b.push([this.price(lifted), q.toFixed(8)]);

      this.bidTopIdx = lifted;

      // Trim the far end of the bids and extend the far end of the asks so the
      // maintained depth stays constant.
      const dropBid = this.bidTopIdx - depth;
      if (this.trueBids.delete(dropBid)) b.push([this.price(dropBid), "0"]);

      const newAsk = this.bidTopIdx + depth;
      const qa = this.randQty();
      this.trueAsks.set(newAsk, qa);
      a.push([this.price(newAsk), qa.toFixed(8)]);
    } else {
      // Price down: the best bid is hit and becomes the new best ask.
      const hit = this.bidTopIdx;
      this.trueBids.delete(hit);
      b.push([this.price(hit), "0"]);

      const q = this.randQty();
      this.trueAsks.set(hit, q);
      a.push([this.price(hit), q.toFixed(8)]);

      this.bidTopIdx = hit - 1;

      const dropAsk = this.bidTopIdx + 1 + depth;
      if (this.trueAsks.delete(dropAsk)) a.push([this.price(dropAsk), "0"]);

      const newBid = this.bidTopIdx - depth + 1;
      const qb = this.randQty();
      this.trueBids.set(newBid, qb);
      b.push([this.price(newBid), qb.toFixed(8)]);
    }
  }
}
