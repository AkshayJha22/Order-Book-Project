import type { Level } from "./types.js";

/**
 * An L2 (price-aggregated) order book.
 *
 * Storage is a Map keyed by numeric price, which gives O(1) apply on the hot
 * path. Reads need sorted order, so we keep a dirty flag and re-sort lazily —
 * at most once per read, and only if something actually changed. With ~1000
 * levels per side and a 20 Hz read rate that is ~40k comparisons/sec, which is
 * noise. A production book would use a bucketed price ladder or a skip list to
 * make reads O(N) in the levels requested rather than O(n log n) in the whole
 * book; the tradeoff is worth measuring before paying for it.
 */
export class OrderBook {
  private bids = new Map<number, number>();
  private asks = new Map<number, number>();

  /** Exchange update ID that this book state reflects. */
  lastUpdateId = 0;

  private dirty = true;
  private sortedBids: Level[] = [];
  private sortedAsks: Level[] = [];

  clear(): void {
    this.bids.clear();
    this.asks.clear();
    this.lastUpdateId = 0;
    this.dirty = true;
    this.sortedBids = [];
    this.sortedAsks = [];
  }

  applySnapshot(bids: [string, string][], asks: [string, string][], lastUpdateId: number): void {
    this.bids.clear();
    this.asks.clear();
    this.applySide(this.bids, bids);
    this.applySide(this.asks, asks);
    this.lastUpdateId = lastUpdateId;
    this.dirty = true;
  }

  applyDelta(bids: [string, string][], asks: [string, string][], finalUpdateId: number): void {
    this.applySide(this.bids, bids);
    this.applySide(this.asks, asks);
    this.lastUpdateId = finalUpdateId;
    this.dirty = true;
  }

  private applySide(side: Map<number, number>, updates: [string, string][]): void {
    for (const entry of updates) {
      const price = Number(entry[0]);
      const qty = Number(entry[1]);
      if (!Number.isFinite(price) || !Number.isFinite(qty)) continue;
      // A quantity of zero is the exchange's way of saying "this level is gone".
      // Treating it as a level with size 0 is the classic bug: it leaves a
      // phantom level that will eventually cross the book.
      if (qty <= 0) side.delete(price);
      else side.set(price, qty);
    }
  }

  private resort(): void {
    // Bids descend (best = highest), asks ascend (best = lowest).
    this.sortedBids = [...this.bids.entries()].sort((a, b) => b[0] - a[0]);
    this.sortedAsks = [...this.asks.entries()].sort((a, b) => a[0] - b[0]);
    this.dirty = false;
  }

  /** Top N levels per side, best-first. */
  top(n: number): { bids: Level[]; asks: Level[] } {
    if (this.dirty) this.resort();
    return {
      bids: this.sortedBids.slice(0, n),
      asks: this.sortedAsks.slice(0, n),
    };
  }

  get bestBid(): number | null {
    if (this.dirty) this.resort();
    return this.sortedBids[0]?.[0] ?? null;
  }

  get bestAsk(): number | null {
    if (this.dirty) this.resort();
    return this.sortedAsks[0]?.[0] ?? null;
  }

  get levelCount(): { bid: number; ask: number } {
    return { bid: this.bids.size, ask: this.asks.size };
  }

  /**
   * Invariant: the best bid must be strictly below the best ask. If it isn't,
   * our book has diverged from the exchange's — almost always because we
   * silently dropped or misordered an update. This is a cheap runtime assertion
   * that turns a slow, invisible corruption into a loud, immediate resync.
   */
  isCrossed(): boolean {
    const bb = this.bestBid;
    const ba = this.bestAsk;
    if (bb === null || ba === null) return false;
    return bb >= ba;
  }

  /** Cumulative quantity out to each of the top N levels, for the depth curve. */
  cumulative(levels: Level[]): { price: number; cum: number }[] {
    const out: { price: number; cum: number }[] = [];
    let cum = 0;
    for (const [price, qty] of levels) {
      cum += qty;
      out.push({ price, cum });
    }
    return out;
  }
}
