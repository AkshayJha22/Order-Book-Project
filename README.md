# orderbook-live

A live L2 order book: exchange feed reconciliation, conflated fan-out to browser
clients, and a canvas renderer that holds 60fps under a feed rate the DOM cannot
survive.

The interesting part is not "it displays an order book." It is the three places
where the obvious implementation is quietly wrong.

```
Binance depth stream ──┐
                       ├─> BookSynchronizer ─> OrderBook ─> Hub ─> browsers
REST depth snapshot ───┘     (sequence          (replica)   (conflating
                              reconciliation)                fan-out)
```

---

## Quick start

```bash
npm install
npm run dev            # live Binance BTCUSDT feed
```

Open http://localhost:8080.

```bash
npm run synthetic      # local simulator at 500 events/sec
npm run chaos          # simulator that deliberately drops 0.2% of packets
npm test               # unit + integration tests
npm run bench          # load harness
```

| Flag | Default | Meaning |
|---|---|---|
| `--source` | `binance` | `binance` or `synthetic` |
| `--symbol` | `BTCUSDT` | Trading pair |
| `--speed` | `100ms` | `100ms` (10/s) or `realtime` (~1000/s) |
| `--depth` | `20` | Levels per side sent to the browser |
| `--flush-hz` | `20` | Broadcast rate to browsers |
| `--rate` | `500` | Synthetic events/sec |
| `--inject-gaps` | `0` | Probability a synthetic event is silently dropped |
| `--high-water` | `262144` | Socket buffer bytes above which a client is "behind" |

`GET /metrics` returns live counters and percentiles. `GET /healthz` returns 200
only when the feed is synchronized.

---

## The three problems

### 1. A snapshot and a delta stream do not line up

The websocket gives you deltas starting whenever you connected. REST gives you a
snapshot from whenever it was served. Neither alone is a book, and naively
applying deltas to a snapshot gives you a book that is subtly, invisibly wrong.

The reconciliation procedure (`src/sync.ts`):

1. Open the stream and buffer every event. Apply nothing.
2. Fetch the snapshot; note its `lastUpdateId`.
3. Discard buffered events whose final ID `u` is below it — already baked in.
4. The first event applied must **straddle** the snapshot: `U ≤ lastUpdateId+1 ≤ u`.
   Nothing straddles it and events have run ahead? There is a hole. Refetch.
5. Thereafter, every event must satisfy `U == previous_u + 1`.

**Step 5 is the one that gets skipped**, and skipping it does not throw. A lost
delta leaves a stale price level sitting there looking entirely plausible. You
find out minutes later, when the book crosses, and by then the corruption is
untraceable.

There is a second, independent check: after every delta, assert `bestBid < bestAsk`.
A crossed book is physically impossible on a real exchange, so if the replica
ever shows one, the replica is wrong. It is a cheap runtime assertion that turns
slow silent drift into an immediate, loud resync.

### 2. `ws.send()` in a loop is an unbounded queue

```js
for (const client of clients) client.send(frame);   // memory leak
```

`send()` returns immediately whether or not the bytes went anywhere. If a
client's TCP window is full they pile up in the socket's internal buffer. One
laptop on bad wifi and the server's memory grows without limit. Nothing in that
code looks wrong and nothing warns you.

The fix has two halves (`src/hub.ts`):

- **Conflation.** Each client gets a *single-slot mailbox*, not a queue. A new
  frame overwrites whatever was waiting. A client that can absorb two frames per
  second gets the two *newest* frames per second, not the two oldest.
- **Backpressure.** Check `bufferedAmount` before writing. Above high water,
  skip the flush entirely. Stay behind long enough and you get closed with code
  1013 rather than carried forever.

This is only sound **because the wire frame is a self-contained snapshot rather
than a delta.** If we shipped deltas, dropping a frame would silently corrupt
every client that missed it. The wire format and the backpressure strategy are
one decision, not two — that coupling is the actual design insight here.

Serialization is also deliberately at the *flush* rate, not the *event* rate. At
1000 events/sec and a 20Hz broadcast, serializing per event means 98% of the
`JSON.stringify` cost produces frames nobody ever sees.

### 3. Render-per-message dies at 500 messages/sec

At 500 msg/s a render-per-message means ~8 full repaints per frame, seven of
which are discarded before a pixel changes. And 40 price levels updating at 60fps
is 2400 DOM text mutations per second, each a potential layout invalidation.

So: incoming messages mutate a plain object and set a dirty flag; a single
`requestAnimationFrame` loop owns all drawing, to canvas. Render cost becomes
bounded by display refresh rate no matter how fast the feed runs.

---

## Testing: assert against ground truth, not plausibility

`src/synthetic.ts` is a local exchange simulator, and it exists for two reasons
that are not "I couldn't reach Binance":

**Rate.** The public throttled stream is 10 events/sec, which never pressures
anything. Conflation that never conflates is untested code. Here the rate is a
flag.

**Faults on demand.** Correctness hinges on detecting a dropped message, and you
cannot ask a real exchange to drop one for you. `--inject-gaps` mutates the truth
book and advances the update ID, then simply never delivers the event — exactly
what a lost packet looks like from the client's side. "My resync path works"
becomes something demonstrated rather than asserted.

It emits the same event shape and drives the same `BookSynchronizer`, so the
tested path is the production path.

Because the simulator knows ground truth, the strongest assertion is available:

```
✔ replica converges to the simulator's truth book over a clean run
✔ injected packet loss is always detected, never silently absorbed
```

The first compares **every level, not just the top of book** — a replica that is
right at the touch and wrong ten levels deep is still wrong, and that is exactly
the failure an eyeball test misses. The second asserts `crossedBooks === 0`:
sequence checking must catch corruption *before* it ever reaches the point of
crossing.

```
# tests 10
# pass 10
# fail 0
```

---

## Benchmarks

Run them yourself; numbers are hardware-specific and quoting someone else's is
meaningless.

```bash
npm run synthetic &
node bench/loadtest.js --clients=200 --seconds=60 --slow=15
```

| Metric | Value |
|---|---|
| Clients | _fill in_ |
| Feed rate | _fill in_ |
| Fan-out p50 / p99 | _fill in_ |
| Conflation ratio | _fill in_ |
| RSS after 60s | _fill in_ |
| Slow-consumer drops | _fill in_ |

**The run that matters** is `--slow=N`, which makes N clients block their event
loop on every message so they stop draining their socket. `slowConsumerSkips`
should climb while the *healthy* clients' latency stays flat. If a slow client
degrades the fast ones, the fan-out is coupled somewhere it shouldn't be.

**Methodology caveat, stated because it changes how you read the numbers:** the
latency figure is server-send timestamp to client-receive. Run against
localhost, both share a clock and the number is real. Across hosts it silently
includes clock skew, which is routinely larger than the thing being measured.
The browser's displayed latency is measured after the draw calls are issued —
the compositor may put pixels up as much as one vsync later, so treat it as a
lower bound on true glass-to-glass rather than the whole story.

---

## What broke

**The first version had no `U == previous_u + 1` check.** It looked perfect. The
book rendered, the numbers moved, the spread was tight. Then it would cross —
sometimes after four minutes, sometimes after forty — and the state at that point
told you nothing about which update had gone missing, because the missing update
was thousands of events in the past.

Adding gap detection to the synthetic feed turned a heisenbug into a unit test
that fails in 55ms. **The lesson generalizes past order books:** if correctness
depends on not losing messages, the sequence check is not defensive
programming, it is the feature. Everything downstream is built on the assumption
that it is there.

**A smaller one, same shape.** The book stored levels with quantity zero instead
of deleting them, because the exchange encodes "level removed" as a quantity of
zero and `map.set(price, 0)` is the thing your fingers type. Phantom zero-size
levels at the touch, and `bestBid` returning a price nobody was bidding. Both
bugs are one line. Both are invisible until the invariant catches them.

---

## Layout

```
src/
  types.ts       wire protocol
  orderbook.ts   L2 book, lazy sort, crossed-book invariant
  sync.ts        snapshot/delta reconciliation  ← the correctness core
  upstream.ts    Binance transport: sockets, backoff, watchdog
  synthetic.ts   exchange simulator + fault injector
  hub.ts         conflating fan-out with backpressure
  metrics.ts     counters and percentiles
  server.ts      HTTP, WebSocket, wiring
public/          canvas dashboard, no framework
test/            unit + integration
bench/           load harness
```

`sync.ts` deliberately knows nothing about websockets, HTTP, or which exchange
it is talking to. That separation is what lets the simulator drive the real code
path instead of a parallel copy of it.
