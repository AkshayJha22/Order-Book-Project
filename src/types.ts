/**
 * Wire types shared between the feed layer, the fan-out hub, and the browser.
 *
 * Design note: the frame we send to browsers is a *self-contained snapshot* of
 * the top N levels, not a delta. That is deliberate. A delta stream cannot be
 * conflated (you must deliver every message, in order, or the client's state is
 * wrong). A snapshot stream can be: the newest frame supersedes every older one,
 * so a slow client can simply skip frames and stay correct. See hub.ts.
 */

/** A single price level: [price, quantity]. */
export type Level = [price: number, qty: number];

/** Raw depth update as it arrives from the exchange (or the synthetic feed). */
export interface DepthEvent {
  /** Event time, ms since epoch, stamped by the exchange. */
  E: number;
  /** First update ID covered by this event. */
  U: number;
  /** Final update ID covered by this event. */
  u: number;
  /** Bid changes, as [priceStr, qtyStr]. qty "0" means remove the level. */
  b: [string, string][];
  /** Ask changes. */
  a: [string, string][];
}

/** REST snapshot payload. */
export interface DepthSnapshot {
  lastUpdateId: number;
  bids: [string, string][];
  asks: [string, string][];
}

export type FeedState =
  | "connecting"
  | "buffering"
  | "live"
  | "resyncing"
  | "disconnected";

export interface HelloMsg {
  type: "hello";
  symbol: string;
  depth: number;
  source: "binance" | "synthetic";
  flushHz: number;
}

export interface StatusMsg {
  type: "status";
  state: FeedState;
  detail?: string;
  resyncs: number;
  reconnects: number;
}

export interface BookMsg {
  type: "book";
  /** Monotonic server broadcast sequence. Lets the client count skipped frames. */
  seq: number;
  bids: Level[];
  asks: Level[];
  /** Exchange update ID the book state reflects. */
  lastUpdateId: number;
  /** Server clock, ms, stamped immediately before serialization. */
  serverTs: number;
  /** Exchange event clock, ms. Subject to cross-host clock skew. */
  exchangeTs: number;
  /** Total tracked levels per side (not just the top N sent). */
  depthLevels: { bid: number; ask: number };
}

export type ServerMessage = HelloMsg | StatusMsg | BookMsg;
