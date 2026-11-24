/* ---------------------------------------------------------------------------
   Browser client.

   The load-bearing decision here is that incoming messages never touch the DOM
   and never trigger a render. They mutate a plain object and set a dirty flag;
   a single requestAnimationFrame loop owns all drawing. At 500 messages/second
   a render-per-message would mean ~8 full repaints per frame, of which 7 are
   thrown away before a pixel changes. Decoupling ingest from paint means the
   render cost is bounded by the display refresh rate no matter how fast the
   feed goes.

   Everything is drawn to canvas rather than DOM nodes. 40 price levels updating
   at 60fps is 2400 DOM text mutations per second, each one a potential layout
   invalidation; on canvas it is one draw call sequence with no layout at all.
   ------------------------------------------------------------------------ */

const BUDGET_MS = 50; // latency budget; the ribbon turns amber above this
const RIBBON_SAMPLES = 260;
const LAT_WINDOW = 600;

const css = getComputedStyle(document.documentElement);
const C = {
  paper: css.getPropertyValue("--paper").trim() || "#e4e5e0",
  panel: css.getPropertyValue("--panel").trim() || "#f0f0ec",
  ink: css.getPropertyValue("--ink").trim() || "#15171b",
  inkMid: css.getPropertyValue("--ink-mid").trim() || "#5c616b",
  inkFaint: css.getPropertyValue("--ink-faint").trim() || "#9aa0a8",
  rule: css.getPropertyValue("--rule").trim() || "#c6c8c1",
  bid: css.getPropertyValue("--bid").trim() || "#1b4d89",
  ask: css.getPropertyValue("--ask").trim() || "#a8362a",
  warn: css.getPropertyValue("--warn").trim() || "#c77b1f",
};

const MONO = '"IBM Plex Mono", ui-monospace, monospace';
const COND = '"IBM Plex Sans Condensed", Arial, sans-serif';

// --- state ------------------------------------------------------------------

const state = {
  symbol: "—",
  source: "",
  feedState: "connecting",
  feedDetail: "",

  bids: [],
  asks: [],
  lastUpdateId: 0,
  depthLevels: { bid: 0, ask: 0 },

  lastSeq: 0,
  skipped: 0,

  /** Server send timestamp awaiting a paint, or null. */
  pendingTs: null,
  latencies: [],
  ribbon: new Float32Array(RIBBON_SAMPLES),
  ribbonHead: 0,
  ribbonFilled: 0,

  dirty: true,
  msgCount: 0,
  msgRate: 0,
  frameCount: 0,
  fps: 0,
  lastRateAt: performance.now(),

  maxQtySmoothed: 1,
};

// --- websocket --------------------------------------------------------------

const el = (id) => document.getElementById(id);
let ws = null;
let reconnectAttempt = 0;

function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/stream`);

  ws.onopen = () => {
    reconnectAttempt = 0;
  };

  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    if (msg.type === "book") {
      // Count frames the server conflated away or that we never received. This
      // is expected under load — it is what conflation does — so it is
      // reported, not treated as an error.
      if (state.lastSeq && msg.seq > state.lastSeq + 1) {
        state.skipped += msg.seq - state.lastSeq - 1;
      }
      state.lastSeq = msg.seq;
      state.bids = msg.bids;
      state.asks = msg.asks;
      state.lastUpdateId = msg.lastUpdateId;
      state.depthLevels = msg.depthLevels;
      state.pendingTs = msg.serverTs;
      state.msgCount++;
      state.dirty = true;
    } else if (msg.type === "status") {
      state.feedState = msg.state;
      state.feedDetail = msg.detail ?? "";
      paintStatus();
    } else if (msg.type === "hello") {
      state.symbol = msg.symbol;
      state.source = msg.source;
      el("symbol").textContent = msg.symbol;
      el("source").textContent = `${msg.source} · ${msg.flushHz}hz · ${msg.depth} levels`;
    }
  };

  ws.onclose = (ev) => {
    state.feedState = ev.code === 1013 ? "disconnected" : "disconnected";
    state.feedDetail =
      ev.code === 1013 ? "dropped: this client could not keep up" : "socket closed";
    paintStatus();
    // Full jitter, same reasoning as the server: a server restart otherwise
    // brings every open tab back in lockstep.
    const ceiling = Math.min(10000, 250 * 2 ** reconnectAttempt);
    reconnectAttempt = Math.min(reconnectAttempt + 1, 8);
    setTimeout(connect, Math.random() * ceiling);
  };

  ws.onerror = () => ws.close();
}

function paintStatus() {
  el("lamp").dataset.state = state.feedState;
  el("stateText").textContent = state.feedState;
  el("stateDetail").textContent = state.feedDetail;
}

// --- canvas plumbing --------------------------------------------------------

function fitCanvas(canvas) {
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  const w = Math.max(1, Math.floor(rect.width * dpr));
  const h = Math.max(1, Math.floor(rect.height * dpr));
  if (canvas.width !== w || canvas.height !== h) {
    canvas.width = w;
    canvas.height = h;
  }
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, w: rect.width, h: rect.height };
}

const fmtPrice = (p) => p.toFixed(2);
const fmtQty = (q) => (q >= 1000 ? q.toFixed(0) : q.toFixed(4));

// --- ribbon -----------------------------------------------------------------

function drawRibbon() {
  const { ctx, w, h } = fitCanvas(el("ribbon"));
  ctx.clearRect(0, 0, w, h);

  const n = state.ribbonFilled;
  if (n === 0) return;

  // Scale to the window max, floored at the budget so the budget line stays on
  // screen and small numbers don't get visually amplified into looking bad.
  let max = BUDGET_MS;
  for (let i = 0; i < n; i++) max = Math.max(max, state.ribbon[i]);
  max *= 1.15;

  const barW = w / RIBBON_SAMPLES;

  for (let i = 0; i < n; i++) {
    // Read oldest-first so the ribbon scrolls left to right.
    const idx = (state.ribbonHead - n + i + RIBBON_SAMPLES * 2) % RIBBON_SAMPLES;
    const v = state.ribbon[idx];
    const barH = Math.max(1, (v / max) * (h - 8));
    const x = (i + (RIBBON_SAMPLES - n)) * barW;
    ctx.fillStyle = v > BUDGET_MS ? C.warn : C.inkMid;
    ctx.globalAlpha = v > BUDGET_MS ? 0.95 : 0.55;
    ctx.fillRect(x, h - barH, Math.max(1, barW - 0.6), barH);
  }
  ctx.globalAlpha = 1;

  // Budget line.
  const by = h - (BUDGET_MS / max) * (h - 8);
  ctx.strokeStyle = C.rule;
  ctx.setLineDash([3, 3]);
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(0, by + 0.5);
  ctx.lineTo(w, by + 0.5);
  ctx.stroke();
  ctx.setLineDash([]);

  ctx.font = `600 9px ${COND}`;
  ctx.fillStyle = C.inkFaint;
  ctx.textAlign = "left";
  ctx.fillText(`${BUDGET_MS}MS BUDGET`, 2, by - 3);
}

// --- ladder -----------------------------------------------------------------

function drawLadder(ctx, x, y, w, h) {
  const rowH = 17;
  const headH = 16;
  const spreadH = 22;
  const avail = h - headH - spreadH;
  const perSide = Math.max(1, Math.floor(avail / 2 / rowH));

  const asks = state.asks.slice(0, perSide);
  const bids = state.bids.slice(0, perSide);

  // Smooth the bar scale so a single large order doesn't make every other bar
  // collapse for one frame and snap back on the next.
  let frameMax = 0;
  for (const [, q] of asks) frameMax = Math.max(frameMax, q);
  for (const [, q] of bids) frameMax = Math.max(frameMax, q);
  if (frameMax > 0) {
    state.maxQtySmoothed += (frameMax - state.maxQtySmoothed) * 0.12;
  }
  const maxQ = Math.max(state.maxQtySmoothed, 1e-9);

  const colPrice = x + 6;
  const colQty = x + w * 0.62;
  const colCum = x + w - 6;

  // Header
  ctx.font = `600 9px ${COND}`;
  ctx.fillStyle = C.inkFaint;
  ctx.textAlign = "left";
  ctx.fillText("PRICE", colPrice, y + 10);
  ctx.textAlign = "right";
  ctx.fillText("SIZE", colQty, y + 10);
  ctx.fillText("CUMULATIVE", colCum, y + 10);

  ctx.strokeStyle = C.rule;
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(x, y + headH - 0.5);
  ctx.lineTo(x + w, y + headH - 0.5);
  ctx.stroke();

  const drawRows = (levels, topY, color, ascending) => {
    // `ascending` false means row 0 is drawn at the bottom of the block, which
    // is what puts the best price adjacent to the spread on both sides.
    let cum = 0;
    const cums = levels.map(([, q]) => (cum += q));

    for (let i = 0; i < levels.length; i++) {
      const [price, qty] = levels[i];
      const slot = ascending ? i : levels.length - 1 - i;
      const ry = topY + slot * rowH;

      // Depth bar, anchored right, proportional to size at this level.
      const bw = Math.min(w, (qty / maxQ) * w * 0.92);
      ctx.fillStyle = color;
      ctx.globalAlpha = 0.13;
      ctx.fillRect(x + w - bw, ry + 1, bw, rowH - 2);
      ctx.globalAlpha = 1;

      ctx.font = `500 11.5px ${MONO}`;
      ctx.fillStyle = color;
      ctx.textAlign = "left";
      ctx.fillText(fmtPrice(price), colPrice, ry + 12);

      ctx.fillStyle = C.ink;
      ctx.textAlign = "right";
      ctx.fillText(fmtQty(qty), colQty, ry + 12);

      ctx.fillStyle = C.inkFaint;
      ctx.fillText(fmtQty(cums[i]), colCum, ry + 12);
    }
  };

  const asksTop = y + headH;
  // Asks: index 0 is the best (lowest) ask, drawn at the BOTTOM of the block.
  drawRows(asks, asksTop, C.ask, false);

  // Spread band
  const spreadY = asksTop + perSide * rowH;
  const bb = bids[0]?.[0];
  const ba = asks[0]?.[0];
  ctx.fillStyle = C.panel;
  ctx.fillRect(x, spreadY, w, spreadH);
  ctx.strokeStyle = C.rule;
  ctx.beginPath();
  ctx.moveTo(x, spreadY + 0.5);
  ctx.lineTo(x + w, spreadY + 0.5);
  ctx.moveTo(x, spreadY + spreadH - 0.5);
  ctx.lineTo(x + w, spreadY + spreadH - 0.5);
  ctx.stroke();

  if (bb != null && ba != null) {
    const spread = ba - bb;
    const mid = (ba + bb) / 2;
    const bps = (spread / mid) * 10000;
    ctx.font = `600 11px ${MONO}`;
    ctx.fillStyle = C.ink;
    ctx.textAlign = "left";
    ctx.fillText(fmtPrice(mid), colPrice, spreadY + 15);
    ctx.textAlign = "right";
    ctx.fillStyle = C.inkMid;
    ctx.font = `500 10.5px ${MONO}`;
    ctx.fillText(`${spread.toFixed(2)}  ${bps.toFixed(2)}bp`, colCum, spreadY + 15);
  }

  // Bids: index 0 is the best (highest) bid, drawn at the TOP of its block.
  drawRows(bids, spreadY + spreadH, C.bid, true);
}

// --- depth curve ------------------------------------------------------------

function drawDepth(ctx, x, y, w, h) {
  const bids = state.bids;
  const asks = state.asks;
  if (bids.length < 2 || asks.length < 2) return;

  const padT = 18;
  const padB = 16;
  const plotY = y + padT;
  const plotH = h - padT - padB;

  const loP = bids[bids.length - 1][0];
  const hiP = asks[asks.length - 1][0];
  const span = hiP - loP || 1;

  let cumB = 0;
  const bidPts = bids.map(([p, q]) => ({ p, c: (cumB += q) }));
  let cumA = 0;
  const askPts = asks.map(([p, q]) => ({ p, c: (cumA += q) }));
  const maxC = Math.max(cumB, cumA) || 1;

  const px = (p) => x + ((p - loP) / span) * w;
  const py = (c) => plotY + plotH - (c / maxC) * plotH;

  // Frame
  ctx.strokeStyle = C.rule;
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(x, plotY + plotH + 0.5);
  ctx.lineTo(x + w, plotY + plotH + 0.5);
  ctx.stroke();

  const stepArea = (pts, color, dir) => {
    ctx.beginPath();
    ctx.moveTo(px(pts[0].p), plotY + plotH);
    let prevY = py(pts[0].c);
    ctx.lineTo(px(pts[0].p), prevY);
    for (let i = 1; i < pts.length; i++) {
      const cx = px(pts[i].p);
      // Step, not slope: depth is a piecewise-constant function of price, and
      // drawing it as a smooth line implies liquidity that isn't there.
      ctx.lineTo(cx, prevY);
      prevY = py(pts[i].c);
      ctx.lineTo(cx, prevY);
    }
    ctx.lineTo(px(pts[pts.length - 1].p), plotY + plotH);
    ctx.closePath();
    ctx.fillStyle = color;
    ctx.globalAlpha = 0.16;
    ctx.fill();
    ctx.globalAlpha = 1;
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.4;
    ctx.stroke();
    void dir;
  };

  stepArea(bidPts, C.bid, -1);
  stepArea(askPts, C.ask, 1);

  // Mid marker
  const mid = (bids[0][0] + asks[0][0]) / 2;
  ctx.strokeStyle = C.inkFaint;
  ctx.setLineDash([2, 3]);
  ctx.beginPath();
  ctx.moveTo(px(mid), plotY);
  ctx.lineTo(px(mid), plotY + plotH);
  ctx.stroke();
  ctx.setLineDash([]);

  // Axis labels
  ctx.font = `600 9px ${COND}`;
  ctx.fillStyle = C.inkFaint;
  ctx.textAlign = "left";
  ctx.fillText(fmtPrice(loP), x, plotY + plotH + 12);
  ctx.textAlign = "right";
  ctx.fillText(fmtPrice(hiP), x + w, plotY + plotH + 12);
  ctx.textAlign = "left";
  ctx.fillText(`CUMULATIVE DEPTH · MAX ${fmtQty(maxC)}`, x, y + 10);

  ctx.textAlign = "center";
  ctx.fillStyle = C.inkMid;
  ctx.font = `500 10px ${MONO}`;
  ctx.fillText(fmtPrice(mid), px(mid), plotY - 4);
}

// --- book canvas ------------------------------------------------------------

function drawBook() {
  const { ctx, w, h } = fitCanvas(el("book"));
  ctx.clearRect(0, 0, w, h);

  if (state.bids.length === 0 && state.asks.length === 0) {
    ctx.font = `600 11px ${COND}`;
    ctx.fillStyle = C.inkFaint;
    ctx.textAlign = "center";
    ctx.fillText("WAITING FOR THE FIRST SYNCHRONIZED BOOK", w / 2, h / 2);
    return;
  }

  const stacked = w < 760;
  if (stacked) {
    const ladderH = Math.min(h * 0.62, h - 130);
    drawLadder(ctx, 0, 0, w, ladderH);
    drawDepth(ctx, 0, ladderH + 10, w, h - ladderH - 10);
  } else {
    const gap = 28;
    const ladderW = Math.min(340, w * 0.44);
    drawLadder(ctx, 0, 0, ladderW, h);
    drawDepth(ctx, ladderW + gap, 0, w - ladderW - gap, h);
  }
}

// --- render loop ------------------------------------------------------------

function percentile(sorted, q) {
  if (sorted.length === 0) return 0;
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
}

function updateStats(now) {
  state.frameCount++;
  const elapsed = now - state.lastRateAt;
  if (elapsed < 1000) return;

  state.fps = Math.round((state.frameCount * 1000) / elapsed);
  state.msgRate = Math.round((state.msgCount * 1000) / elapsed);
  state.frameCount = 0;
  state.msgCount = 0;
  state.lastRateAt = now;

  const sorted = [...state.latencies].sort((a, b) => a - b);
  el("latP50").textContent = percentile(sorted, 0.5).toFixed(1);
  el("latP99").textContent = percentile(sorted, 0.99).toFixed(1);
  el("latMax").textContent = (sorted[sorted.length - 1] ?? 0).toFixed(1);
  el("fps").textContent = state.fps;
  el("rate").textContent = state.msgRate;
  el("skipped").textContent = state.skipped;
}

function frame(now) {
  requestAnimationFrame(frame);

  // The ribbon is always redrawn (it scrolls); the book only when data changed.
  if (state.dirty) {
    drawBook();
    state.dirty = false;

    if (state.pendingTs !== null) {
      // Measured after the draw calls are issued. The compositor may put the
      // pixels up as much as one vsync later, so treat this as a lower bound
      // on true glass-to-glass rather than the whole story.
      const lat = Date.now() - state.pendingTs;
      state.pendingTs = null;
      if (lat >= 0 && lat < 60000) {
        state.latencies.push(lat);
        if (state.latencies.length > LAT_WINDOW) state.latencies.shift();
        state.ribbon[state.ribbonHead] = lat;
        state.ribbonHead = (state.ribbonHead + 1) % RIBBON_SAMPLES;
        state.ribbonFilled = Math.min(state.ribbonFilled + 1, RIBBON_SAMPLES);
      }
    }
  }

  drawRibbon();
  updateStats(now);
}

// --- boot -------------------------------------------------------------------

async function main() {
  // Canvas measures text against whatever font is loaded at draw time. Starting
  // before the webfont resolves means the first frames are laid out for a
  // fallback and visibly reflow when Plex arrives.
  try {
    await document.fonts.ready;
  } catch {
    /* no font loading API: proceed with fallbacks */
  }
  paintStatus();
  connect();
  requestAnimationFrame(frame);
  window.addEventListener("resize", () => {
    state.dirty = true;
  });
}

main();
