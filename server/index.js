'use strict';

const express    = require('express');
const cors       = require('cors');
const http       = require('http');
const WebSocket  = require('ws');
const fetch      = require('node-fetch');
const path       = require('path');
const { computeSignal, SPIKE_COOLDOWN } = require('./bot');

const PORT            = process.env.PORT || 3000;
const INITIAL_BALANCE = 10000;
const MAX_PRICES      = 1000;  // store more prices for better indicators

// Bot runs signal logic every N ms — not on every tick
// Real-time price is used for SL/TP checks on EVERY tick
const BOT_LOGIC_INTERVAL = 1000; // 1 second — strategy evaluation
const BROADCAST_INTERVAL = 2000; // 2 seconds — push to dashboard

// ─── IN-MEMORY STATE ─────────────────────────────────────────
let state = {
  version:      'V1-RT',        // RT = real-time
  startedAt:    null,
  price:        0,
  prevPrice:    0,
  change24h:    0,
  balance:      INITIAL_BALANCE,
  equity:       INITIAL_BALANCE,
  position:     null,
  prices:       [],             // 1s sampled prices for indicators
  equityHistory:[INITIAL_BALANCE],
  cooldown:     0,
  regime:       'NORMAL',
  wins:         0,
  losses:       0,
  bestTrade:    0,
  worstTrade:   0,
  posSize:      10,
  slPct:        0.02,
  tpPct:        0.04,
  running:      false,
  tickCount:    0,              // total price ticks received
  lastTick:     null,
  wsConnected:  false,
};
let trades   = [];
let lastSig  = { action:'wait', signals:[50,50,50,50,50], regime:'NORMAL', chaosFilter:1 };

// ─── BINANCE REAL-TIME WEBSOCKET ─────────────────────────────
// wss://stream.binance.com:9443/ws/btcusdt@aggTrade
// Fires on every aggregated trade — roughly every 100ms
// We use it for:
//   1. Accurate real-time price (for SL/TP)
//   2. 1-second sampled price array (for indicators)

let binanceWs        = null;
let reconnectTimer   = null;
let lastSampleTime   = 0;
let lastBotLogicTime = 0;
let lastBroadcastTime = 0;

function connectBinance() {
  if (binanceWs) {
    try { binanceWs.terminate(); } catch(e) {}
  }

  console.log('[BINANCE] Connecting to real-time stream...');
  binanceWs = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@aggTrade');

  binanceWs.on('open', () => {
    console.log('[BINANCE] ✓ Real-time stream connected');
    state.wsConnected = true;
    clearTimeout(reconnectTimer);
    // Also fetch 24h change once on connect
    fetch24hChange();
  });

  binanceWs.on('message', (data) => {
    try {
      const msg   = JSON.parse(data);
      const price = parseFloat(msg.p); // trade price
      if (!price || isNaN(price)) return;

      state.prevPrice = state.price;
      state.price     = price;
      state.tickCount++;
      state.lastTick  = new Date().toISOString();

      const now = Date.now();

      // ── 1. REAL-TIME SL/TP CHECK (every tick ~100ms) ──────
      // This is the key improvement over polling —
      // SL/TP fires at the actual price, not 10s later
      if (state.running && state.position) {
        checkSLTP(price);
      }

      // ── 2. SAMPLE PRICE every 1 second for indicators ─────
      if (now - lastSampleTime >= 1000) {
        lastSampleTime = now;
        state.prices.push(price);
        if (state.prices.length > MAX_PRICES) state.prices.shift();
      }

      // ── 3. RUN BOT LOGIC every BOT_LOGIC_INTERVAL ─────────
      if (state.running && now - lastBotLogicTime >= BOT_LOGIC_INTERVAL) {
        lastBotLogicTime = now;
        runBotLogic(price);
      }

      // ── 4. BROADCAST to dashboard every 2 seconds ─────────
      if (now - lastBroadcastTime >= BROADCAST_INTERVAL) {
        lastBroadcastTime = now;
        broadcast({ type: 'tick', state: publicState(), sig: lastSig });
      }

    } catch(e) {}
  });

  binanceWs.on('close', () => {
    console.log('[BINANCE] Stream closed — reconnecting in 3s...');
    state.wsConnected = false;
    reconnectTimer = setTimeout(connectBinance, 3000);
  });

  binanceWs.on('error', (err) => {
    console.log('[BINANCE] Stream error:', err.message);
    state.wsConnected = false;
  });
}

// ─── REAL-TIME SL/TP CHECK ────────────────────────────────────
// Runs on every price tick for accurate fills
function checkSLTP(price) {
  if (!state.position) return;
  const p      = state.position;
  const pnl    = p.type === 'long' ? (price - p.entry)*p.qty : (p.entry - price)*p.qty;
  const pnlPct = pnl / (p.entry * p.qty);

  const regime = state.regime;
  const dynSL  = state.slPct * (regime === 'EXTREME' ? 0.4 : regime === 'VOLATILE' ? 0.6 : 1);
  const dynTP  = state.tpPct * (regime === 'EXTREME' ? 0.5 : regime === 'VOLATILE' ? 0.7 : 1);

  if      (pnlPct <= -dynSL) closeTrade(pnl, 'SL', lastSig);
  else if (pnlPct >=  dynTP) closeTrade(pnl, 'TP', lastSig);
}

// ─── BOT LOGIC (runs every 1s on sampled prices) ──────────────
function runBotLogic(price) {
  if (state.prices.length < 30) return;
  if (state.cooldown > 0) state.cooldown--;

  const sig = computeSignal(state.prices, state.cooldown);
  lastSig       = sig;
  state.regime  = sig.regime;

  if (sig.spike) {
    state.cooldown = SPIKE_COOLDOWN;
    console.log(`[SPIKE] Detected at $${price.toFixed(2)} — cooldown ${SPIKE_COOLDOWN}s`);
  }

  // Reversal exit (SL/TP handled in real-time above)
  if (state.position) {
    const p = state.position;
    if (sig.action !== 'wait' &&
        ((p.type === 'long'  && sig.action === 'short') ||
         (p.type === 'short' && sig.action === 'long'))) {
      const pnl = p.type === 'long'
        ? (price - p.entry) * p.qty
        : (p.entry - price) * p.qty;
      closeTrade(pnl, 'REV', sig);
    }
  }

  // Open new position
  const effSize = state.balance * (state.posSize / 100) * sig.chaosFilter;
  if (!state.position && sig.chaosFilter > 0 &&
      (sig.action === 'long' || sig.action === 'short') &&
      state.balance > 200 && effSize > 50) {
    state.position = {
      type:     sig.action,
      entry:    price,
      size:     effSize,
      qty:      effSize / price,
      openedAt: new Date().toISOString(),
    };
    console.log(`[OPEN] ${sig.action.toUpperCase()} @ $${price.toFixed(2)} | Size: $${effSize.toFixed(0)} | Regime: ${sig.regime}`);
  }

  // Update equity
  let unreal = 0;
  if (state.position) {
    unreal = state.position.type === 'long'
      ? (price - state.position.entry) * state.position.qty
      : (state.position.entry - price) * state.position.qty;
  }
  state.equity = state.balance + unreal;
  state.equityHistory.push(state.equity);
  if (state.equityHistory.length > 86400) state.equityHistory.shift(); // 24h of 1s data
}

function closeTrade(pnl, reason, sig) {
  if (!state.position) return;
  const p = state.position;
  state.balance += pnl;
  const trade = {
    id:       trades.length + 1,
    version:  'V1-RT',
    type:     p.type,
    entry:    p.entry,
    exit:     state.price,
    size:     p.size,
    qty:      p.qty,
    pnl:      parseFloat(pnl.toFixed(4)),
    pnlPct:   parseFloat((pnl / p.size * 100).toFixed(3)),
    reason,
    regime:   sig.regime || state.regime,
    rsi:      sig.rsi ? parseFloat(sig.rsi.toFixed(2)) : null,
    openedAt: p.openedAt,
    closedAt: new Date().toISOString(),
    balance:  parseFloat(state.balance.toFixed(2)),
  };
  trades.push(trade);
  pnl > 0 ? state.wins++ : state.losses++;
  if (pnl > state.bestTrade)  state.bestTrade  = pnl;
  if (pnl < state.worstTrade) state.worstTrade = pnl;
  state.position = null;
  const emoji = pnl >= 0 ? '✓' : '✗';
  console.log(`[CLOSE] ${emoji} ${trade.type.toUpperCase()} | ${reason} | PnL: ${pnl>=0?'+':''}$${pnl.toFixed(2)} | Balance: $${state.balance.toFixed(2)}`);
  broadcast({ type: 'trade', trade });
}

// ─── 24H CHANGE (fetch once per hour) ────────────────────────
async function fetch24hChange() {
  try {
    const r = await fetch('https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT', { timeout: 5000 });
    const d = await r.json();
    state.change24h = parseFloat(d.priceChangePercent);
    console.log(`[24H] Change: ${state.change24h.toFixed(2)}%`);
  } catch(e) {}
}
setInterval(fetch24hChange, 3600000); // every hour

// ─── PUBLIC STATE ─────────────────────────────────────────────
function publicState() {
  return {
    ...state,
    equityHistory: state.equityHistory.slice(-500),
    prices:        state.prices.slice(-200),
    tradeCount:    trades.length,
  };
}

// ─── ANALYSIS ────────────────────────────────────────────────
function analysePerformance() {
  if (trades.length === 0) return { error: 'No trades yet' };
  const total  = trades.length;
  const wins   = trades.filter(t => t.pnl > 0);
  const losses = trades.filter(t => t.pnl < 0);
  const totalPnl = trades.reduce((s,t) => s+t.pnl, 0);
  const avgWin   = wins.length   ? wins.reduce((s,t)=>s+t.pnl,0)/wins.length   : 0;
  const avgLoss  = losses.length ? losses.reduce((s,t)=>s+t.pnl,0)/losses.length : 0;
  const pf = avgLoss !== 0 ? Math.abs(avgWin/avgLoss) : 999;

  const byRegime = {}, byReason = {};
  for (const t of trades) {
    if (!byRegime[t.regime]) byRegime[t.regime] = { count:0, pnl:0, wins:0 };
    byRegime[t.regime].count++; byRegime[t.regime].pnl += t.pnl;
    if (t.pnl > 0) byRegime[t.regime].wins++;
    if (!byReason[t.reason]) byReason[t.reason] = { count:0, pnl:0 };
    byReason[t.reason].count++; byReason[t.reason].pnl += t.pnl;
  }

  let peak=INITIAL_BALANCE, maxDD=0, maxCL=0, curCL=0;
  for (const eq of state.equityHistory) {
    if (eq > peak) peak = eq;
    const dd = (peak-eq)/peak*100;
    if (dd > maxDD) maxDD = dd;
  }
  for (const t of trades) {
    if (t.pnl < 0) { curCL++; maxCL=Math.max(maxCL,curCL); } else curCL=0;
  }

  const longs  = trades.filter(t=>t.type==='long');
  const shorts = trades.filter(t=>t.type==='short');

  // Duration analysis
  const durations = trades.map(t => {
    const ms = new Date(t.closedAt) - new Date(t.openedAt);
    return ms / 60000; // minutes
  });
  const avgDuration = durations.reduce((a,b)=>a+b,0) / durations.length;

  return {
    summary: {
      total, winRate: (wins.length/total*100).toFixed(1)+'%',
      totalPnl: +totalPnl.toFixed(2),
      totalPnlPct: +((totalPnl/INITIAL_BALANCE)*100).toFixed(2),
      avgWin: +avgWin.toFixed(2), avgLoss: +avgLoss.toFixed(2),
      profitFactor: +pf.toFixed(2),
      maxDrawdownPct: +maxDD.toFixed(2), maxConsecLoss: maxCL,
      avgTradeDurationMins: +avgDuration.toFixed(1),
      totalTicksReceived: state.tickCount,
    },
    byRegime: Object.entries(byRegime).map(([k,v])=>({
      regime:k, ...v, pnl:+v.pnl.toFixed(2), winRate:+(v.wins/v.count*100).toFixed(1),
    })),
    byReason: Object.entries(byReason).map(([k,v])=>({
      reason:k, ...v, pnl:+v.pnl.toFixed(2),
    })),
    byDirection: {
      longs:  { count:longs.length,  pnl:+longs.reduce((s,t)=>s+t.pnl,0).toFixed(2) },
      shorts: { count:shorts.length, pnl:+shorts.reduce((s,t)=>s+t.pnl,0).toFixed(2) },
    },
    recent: trades.slice(-10),
    allTrades: trades,
  };
}

// ─── EXPRESS + WS ─────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

app.get('/api/state',    (_,res) => res.json(publicState()));
app.get('/api/trades',   (_,res) => res.json(trades));
app.get('/api/analysis', (_,res) => res.json(analysePerformance()));
app.get('/api/health',   (_,res) => res.json({
  ok: true,
  wsConnected: state.wsConnected,
  tickCount: state.tickCount,
  lastTick: state.lastTick,
  running: state.running,
  price: state.price,
}));

app.post('/api/start', (req, res) => {
  if (req.body.posSize) state.posSize = req.body.posSize;
  if (req.body.slPct)   state.slPct   = req.body.slPct;
  if (req.body.tpPct)   state.tpPct   = req.body.tpPct;
  state.running   = true;
  if (!state.startedAt) state.startedAt = new Date().toISOString();
  res.json({ ok: true });
  broadcast({ type: 'tick', state: publicState(), sig: lastSig });
  console.log(`[APEX] Bot started — SL:${(state.slPct*100).toFixed(1)}% TP:${(state.tpPct*100).toFixed(1)}% Size:${state.posSize}%`);
});

app.post('/api/stop', (_,res) => {
  state.running = false;
  res.json({ ok: true });
  console.log('[APEX] Bot stopped');
});

app.post('/api/reset', (_,res) => {
  state.running=false; state.startedAt=null;
  state.balance=INITIAL_BALANCE; state.equity=INITIAL_BALANCE;
  state.position=null; state.prices=[]; state.equityHistory=[INITIAL_BALANCE];
  state.cooldown=0; state.regime='NORMAL';
  state.wins=0; state.losses=0; state.bestTrade=0; state.worstTrade=0;
  state.tickCount=0; state.lastTick=null;
  trades=[];
  broadcast({ type:'reset' });
  res.json({ ok:true });
  console.log('[APEX] Bot reset');
});

function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
}

wss.on('connection', ws => {
  console.log('[WS] Dashboard client connected');
  ws.send(JSON.stringify({ type:'init', state:publicState(), trades:trades.slice(-50) }));
});

// ─── START ────────────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n╔══════════════════════════════════════════╗`);
  console.log(`║  APEX BTC BOT V1-RT — Real-Time Stream  ║`);
  console.log(`║  Port: ${PORT}                              ║`);
  console.log(`╚══════════════════════════════════════════╝\n`);
  // Connect to Binance real-time stream
  connectBinance();
});
