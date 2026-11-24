import { test } from "node:test";
import assert from "node:assert/strict";

import { OrderBook } from "../src/orderbook.js";
import { BookSynchronizer } from "../src/sync.js";
import { SyntheticFeed } from "../src/synthetic.js";
import { Metrics } from "../src/metrics.js";
import type { DepthEvent, DepthSnapshot } from "../src/types.js";

const nextTick = () => new Promise((r) => setImmediate(r));

/** Wait until the synchronizer has drained its snapshot fetch. */
async function settle(rounds = 8): Promise<void> {
  for (let i = 0; i < rounds; i++) await nextTick();
}

// ---------------------------------------------------------------------------
// OrderBook
// ---------------------------------------------------------------------------

test("zero quantity removes a level rather than storing a zero-size one", () => {
  const b = new OrderBook();
  b.applySnapshot([["100.00", "5"]], [["101.00", "5"]], 1);
  assert.equal(b.levelCount.bid, 1);

  b.applyDelta([["100.00", "0"]], [], 2);
  assert.equal(b.levelCount.bid, 0);
  assert.equal(b.bestBid, null);
});

test("best bid is the highest and best ask is the lowest", () => {
  const b = new OrderBook();
  b.applySnapshot(
    [
      ["99.00", "1"],
      ["100.00", "1"],
      ["98.00", "1"],
    ],
    [
      ["103.00", "1"],
      ["101.00", "1"],
      ["102.00", "1"],
    ],
    1,
  );
  assert.equal(b.bestBid, 100);
  assert.equal(b.bestAsk, 101);
  assert.equal(b.isCrossed(), false);
});

test("a crossed book is detected", () => {
  const b = new OrderBook();
  b.applySnapshot([["105.00", "1"]], [["101.00", "1"]], 1);
  assert.equal(b.isCrossed(), true);
});

// ---------------------------------------------------------------------------
// BookSynchronizer — the reconciliation rules
// ---------------------------------------------------------------------------

function harness(snap: DepthSnapshot) {
  const metrics = new Metrics();
  const states: string[] = [];
  const sync = new BookSynchronizer(async () => snap, metrics);
  sync.on("state", (s: string) => states.push(s));
  sync.on("error", () => {});
  return { sync, metrics, states };
}

const ev = (U: number, u: number, b: [string, string][] = [], a: [string, string][] = []): DepthEvent =>
  ({ E: Date.now(), U, u, b, a });

test("events fully contained in the snapshot are discarded as stale", async () => {
  const { sync, metrics } = harness({
    lastUpdateId: 100,
    bids: [["100.00", "1"]],
    asks: [["101.00", "1"]],
  });
  sync.begin();
  sync.push(ev(90, 95, [["100.00", "9"]]));
  await settle();

  assert.equal(metrics.eventsStale, 1);
  assert.equal(metrics.gapsDetected, 0);
  // The stale event must NOT have been applied.
  assert.equal(sync.book.top(1).bids[0]?.[1], 1);
});

test("the first applied event must straddle the snapshot", async () => {
  const { sync, metrics } = harness({
    lastUpdateId: 100,
    bids: [["100.00", "1"]],
    asks: [["101.00", "1"]],
  });
  sync.begin();
  // Straddles 101: U <= 101 <= u.
  sync.push(ev(99, 105, [["100.00", "7"]]));
  await settle();

  assert.equal(metrics.gapsDetected, 0);
  assert.equal(sync.book.lastUpdateId, 105);
  assert.equal(sync.book.top(1).bids[0]?.[1], 7);
});

test("an event that starts beyond the snapshot is a hole, not a resume point", async () => {
  const { sync, metrics } = harness({
    lastUpdateId: 100,
    bids: [["100.00", "1"]],
    asks: [["101.00", "1"]],
  });
  sync.begin();
  // Snapshot ends at 100, this begins at 150: updates 101..149 are missing.
  sync.push(ev(150, 160, [["100.00", "7"]]));
  await settle();

  assert.equal(metrics.gapsDetected, 1);
  assert.ok(metrics.resyncs >= 1, "a hole must force a resync");
});

test("a mid-stream sequence gap is caught and forces a resync", async () => {
  const { sync, metrics } = harness({
    lastUpdateId: 100,
    bids: [["100.00", "1"]],
    asks: [["101.00", "1"]],
  });
  sync.begin();
  sync.push(ev(101, 102, [["100.00", "2"]])); // straddle, applied
  await settle();
  assert.equal(metrics.gapsDetected, 0);

  sync.push(ev(105, 106, [["100.00", "3"]])); // expected U=103, got 105
  await settle();
  assert.equal(metrics.gapsDetected, 1);
  assert.ok(metrics.resyncs >= 1);
});

test("contiguous events apply without triggering a resync", async () => {
  const { sync, metrics } = harness({
    lastUpdateId: 100,
    bids: [["100.00", "1"]],
    asks: [["101.00", "1"]],
  });
  sync.begin();
  await settle();

  let id = 100;
  for (let i = 0; i < 200; i++) {
    const U = id + 1;
    const u = U + 1;
    id = u;
    sync.push(ev(U, u, [["100.00", String(i + 1)]]));
  }
  await settle();

  assert.equal(metrics.gapsDetected, 0);
  assert.equal(metrics.resyncs, 0);
  assert.equal(sync.book.lastUpdateId, id);
});

// ---------------------------------------------------------------------------
// End to end against the simulator
// ---------------------------------------------------------------------------

test("replica converges to the simulator's truth book over a clean run", async () => {
  const metrics = new Metrics();
  const feed = new SyntheticFeed({
    symbol: "TESTUSDT",
    rate: 1,
    depth: 40,
    tickSize: 0.01,
    startPrice: 30000,
    injectGaps: 0,
    metrics,
  });

  feed.on("feed-error", () => {});
  feed.startManual(); // no timer: we drive every event by hand, so the run is deterministic
  await settle(12);

  for (let i = 0; i < 4000; i++) feed.tick();
  await settle(12);

  assert.equal(metrics.crossedBooks, 0, "book must never cross on a clean feed");
  assert.equal(metrics.gapsDetected, 0, "no gaps should appear when none are injected");

  const truth = feed.truth();
  const replica = feed.book;

  assert.equal(replica.levelCount.bid, truth.bids.size, "bid level count must match truth");
  assert.equal(replica.levelCount.ask, truth.asks.size, "ask level count must match truth");

  // Every level, not just the top of book. A replica that is right at the touch
  // and wrong ten levels deep is still wrong, and that is exactly the failure
  // an eyeball test misses.
  const all = replica.top(1e9);
  for (const [price, qty] of truth.bids) {
    const found = all.bids.find(([p]) => Math.abs(p - price) < 1e-9);
    assert.ok(found, `missing bid level ${price}`);
    assert.ok(Math.abs(found[1] - qty) < 1e-6, `bid ${price}: ${found[1]} != ${qty}`);
  }
  for (const [price, qty] of truth.asks) {
    const found = all.asks.find(([p]) => Math.abs(p - price) < 1e-9);
    assert.ok(found, `missing ask level ${price}`);
    assert.ok(Math.abs(found[1] - qty) < 1e-6, `ask ${price}: ${found[1]} != ${qty}`);
  }
});

test("injected packet loss is always detected, never silently absorbed", async () => {
  const metrics = new Metrics();
  const feed = new SyntheticFeed({
    symbol: "TESTUSDT",
    rate: 1,
    depth: 40,
    tickSize: 0.01,
    startPrice: 30000,
    injectGaps: 0.01, // 1% of events never delivered
    metrics,
  });

  feed.on("feed-error", () => {});
  feed.startManual();
  await settle(12);

  for (let i = 0; i < 3000; i++) {
    feed.tick();
    if (i % 100 === 0) await settle(2); // let resync snapshots resolve
  }
  await settle(20);

  assert.ok(feed.droppedEvents > 0, "the injector should have dropped something");

  // The contract: every dropped delta produces a detected gap. Not "most" —
  // an undetected drop means a book that is quietly wrong, which is the exact
  // failure this whole design exists to make impossible.
  assert.ok(
    metrics.gapsDetected > 0,
    `dropped ${feed.droppedEvents} events but detected ${metrics.gapsDetected} gaps`,
  );
  assert.equal(
    metrics.crossedBooks,
    0,
    "sequence checking should catch corruption before it ever crosses the book",
  );
  assert.equal(metrics.resyncs, metrics.gapsDetected, "every gap must trigger exactly one resync");

  feed.stop();
});
