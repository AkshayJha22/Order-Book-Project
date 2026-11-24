/**
 * Minimal metrics. A fixed-size ring buffer of samples, sorted on read.
 *
 * This is honest about its limits: it reports percentiles over the last N
 * samples, not over all time, and sorting on read is O(n log n). At N = 8192
 * and a scrape every few seconds that is irrelevant. If you needed real
 * always-on percentiles you would use an HDR histogram with log-linear buckets;
 * the point here is that the measurement approach is stated, not hidden.
 */
export class Histogram {
  private buf: Float64Array;
  private idx = 0;
  private filled = 0;

  constructor(private capacity = 8192) {
    this.buf = new Float64Array(capacity);
  }

  record(v: number): void {
    this.buf[this.idx] = v;
    this.idx = (this.idx + 1) % this.capacity;
    if (this.filled < this.capacity) this.filled++;
  }

  private samples(): number[] {
    return Array.from(this.buf.subarray(0, this.filled)).sort((a, b) => a - b);
  }

  percentiles(): { n: number; p50: number; p90: number; p99: number; max: number; mean: number } {
    const s = this.samples();
    if (s.length === 0) return { n: 0, p50: 0, p90: 0, p99: 0, max: 0, mean: 0 };
    const at = (q: number) => s[Math.min(s.length - 1, Math.floor(q * s.length))] ?? 0;
    const sum = s.reduce((a, b) => a + b, 0);
    return {
      n: s.length,
      p50: at(0.5),
      p90: at(0.9),
      p99: at(0.99),
      max: s[s.length - 1] ?? 0,
      mean: sum / s.length,
    };
  }

  reset(): void {
    this.idx = 0;
    this.filled = 0;
  }
}

export class Metrics {
  readonly startedAt = Date.now();

  /** Depth events received from the upstream feed. */
  eventsReceived = 0;
  /** Events dropped as stale during snapshot reconciliation. */
  eventsStale = 0;
  /** Sequence gaps detected. Each one forces a resync. */
  gapsDetected = 0;
  /** Full snapshot re-fetches. */
  resyncs = 0;
  /** Upstream socket reconnects. */
  reconnects = 0;
  /** Times the crossed-book invariant tripped. */
  crossedBooks = 0;

  /** Frames handed to the hub for broadcast. */
  framesPublished = 0;
  /** Frames actually written to a client socket. */
  framesSent = 0;
  /** Frames superseded before they were sent (conflation working as intended). */
  framesConflated = 0;
  /** Flushes skipped because a client socket was above the high-water mark. */
  slowConsumerSkips = 0;
  /** Clients disconnected for sustained backpressure. */
  slowConsumerDrops = 0;

  /** Time from receiving an upstream event to publishing a frame, ms. */
  readonly applyLatency = new Histogram();
  /** Frame serialization cost, ms. */
  readonly serializeLatency = new Histogram();

  snapshot(clients: number, feedState: string) {
    const mem = process.memoryUsage();
    return {
      uptimeSec: Math.round((Date.now() - this.startedAt) / 1000),
      feedState,
      clients,
      feed: {
        eventsReceived: this.eventsReceived,
        eventsStale: this.eventsStale,
        gapsDetected: this.gapsDetected,
        resyncs: this.resyncs,
        reconnects: this.reconnects,
        crossedBooks: this.crossedBooks,
      },
      fanout: {
        framesPublished: this.framesPublished,
        framesSent: this.framesSent,
        framesConflated: this.framesConflated,
        conflationRatio:
          this.framesPublished > 0
            ? +(this.framesConflated / this.framesPublished).toFixed(4)
            : 0,
        slowConsumerSkips: this.slowConsumerSkips,
        slowConsumerDrops: this.slowConsumerDrops,
      },
      latencyMs: {
        apply: this.applyLatency.percentiles(),
        serialize: this.serializeLatency.percentiles(),
      },
      memoryMB: {
        rss: +(mem.rss / 1048576).toFixed(1),
        heapUsed: +(mem.heapUsed / 1048576).toFixed(1),
        external: +(mem.external / 1048576).toFixed(1),
      },
    };
  }
}
