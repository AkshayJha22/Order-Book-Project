/**
 * Load harness.
 *
 * Opens N websocket clients, records server-send to client-receive latency for
 * every frame, and reports percentiles alongside the server's own metrics.
 *
 *   node bench/loadtest.js --clients=200 --seconds=60
 *   node bench/loadtest.js --clients=50 --seconds=30 --slow=10
 *
 * `--slow=N` makes N of the clients deliberately pathological: they block the
 * event loop in a tight spin on each message, so they stop draining their
 * socket. That is the scenario the hub's backpressure path exists for, and it
 * is the run where `slowConsumerSkips` and `slowConsumerDrops` should climb
 * while the healthy clients' latency stays flat. If a slow client degrades the
 * fast ones, the fan-out is coupled somewhere it shouldn't be.
 *
 * Caveat worth stating in any writeup: run against localhost, client and server
 * share a clock, so the latency figure is real. Across hosts it silently
 * includes clock skew, which is usually larger than what you're measuring.
 */

import WebSocket from "ws";

const arg = (name, fallback) => {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
};

const CLIENTS = Number(arg("clients", "100"));
const SECONDS = Number(arg("seconds", "30"));
const SLOW = Number(arg("slow", "0"));
const URL = arg("url", "ws://localhost:8080/stream");
const METRICS_URL = arg("metrics", "http://localhost:8080/metrics");

const latencies = [];
let received = 0;
let skipped = 0;
let closedEarly = 0;
const seqByClient = new Map();

function spin(ms) {
  const end = performance.now() + ms;
  while (performance.now() < end) {
    /* deliberately block the event loop */
  }
}

function openClient(i) {
  const isSlow = i < SLOW;
  const ws = new WebSocket(URL);
  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (msg.type !== "book") return;

    received++;
    const prev = seqByClient.get(i);
    if (prev !== undefined && msg.seq > prev + 1) skipped += msg.seq - prev - 1;
    seqByClient.set(i, msg.seq);

    if (!isSlow) latencies.push(Date.now() - msg.serverTs);
    else spin(40); // ~25 messages/sec ceiling: guaranteed to fall behind
  });
  ws.on("close", () => closedEarly++);
  ws.on("error", () => {});
  return ws;
}

function pct(sorted, q) {
  if (!sorted.length) return 0;
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
}

async function main() {
  console.log(
    `opening ${CLIENTS} clients (${SLOW} deliberately slow) against ${URL} for ${SECONDS}s\n`,
  );

  const sockets = [];
  for (let i = 0; i < CLIENTS; i++) {
    sockets.push(openClient(i));
    // Stagger so we measure steady state, not a connection thundering herd.
    if (i % 20 === 19) await new Promise((r) => setTimeout(r, 50));
  }

  const t0 = Date.now();
  await new Promise((r) => setTimeout(r, SECONDS * 1000));
  const elapsed = (Date.now() - t0) / 1000;

  for (const ws of sockets) ws.close();

  const sorted = latencies.slice().sort((a, b) => a - b);
  const mean = sorted.reduce((a, b) => a + b, 0) / (sorted.length || 1);

  console.log("--- client side -------------------------------------------");
  console.log(`clients            ${CLIENTS} (${SLOW} slow)`);
  console.log(`duration           ${elapsed.toFixed(1)}s`);
  console.log(`frames received    ${received}  (${(received / elapsed).toFixed(0)}/s aggregate)`);
  console.log(`frames skipped     ${skipped}  (conflated or dropped upstream)`);
  console.log(`closed early       ${closedEarly}`);
  console.log(`latency samples    ${sorted.length}   [healthy clients only]`);
  console.log(`  mean             ${mean.toFixed(2)} ms`);
  console.log(`  p50              ${pct(sorted, 0.5).toFixed(2)} ms`);
  console.log(`  p90              ${pct(sorted, 0.9).toFixed(2)} ms`);
  console.log(`  p99              ${pct(sorted, 0.99).toFixed(2)} ms`);
  console.log(`  max              ${(sorted[sorted.length - 1] ?? 0).toFixed(2)} ms`);

  try {
    const res = await fetch(METRICS_URL);
    const m = await res.json();
    console.log("\n--- server side -------------------------------------------");
    console.log(`feed state         ${m.feedState}`);
    console.log(`events received    ${m.feed.eventsReceived}`);
    console.log(`gaps detected      ${m.feed.gapsDetected}`);
    console.log(`resyncs            ${m.feed.resyncs}`);
    console.log(`crossed books      ${m.feed.crossedBooks}`);
    console.log(`frames published   ${m.fanout.framesPublished}`);
    console.log(`frames sent        ${m.fanout.framesSent}`);
    console.log(`frames conflated   ${m.fanout.framesConflated}  (ratio ${m.fanout.conflationRatio})`);
    console.log(`slow skips         ${m.fanout.slowConsumerSkips}`);
    console.log(`slow drops         ${m.fanout.slowConsumerDrops}`);
    console.log(`rss                ${m.memoryMB.rss} MB`);
    console.log(`heap used          ${m.memoryMB.heapUsed} MB`);
    if (m.injectedDrops !== undefined) {
      console.log(`injected drops     ${m.injectedDrops}`);
      const caught = m.feed.gapsDetected >= m.injectedDrops * 0.9;
      console.log(`gap detection      ${caught ? "OK" : "UNDER-DETECTING — investigate"}`);
    }
  } catch {
    console.log("\n(could not reach /metrics — server metrics omitted)");
  }

  process.exit(0);
}

main();
