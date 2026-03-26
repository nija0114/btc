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
const MAX_PRICES      = 500;
const PRICE_INTERVAL  = 10000;

// ─── IN-MEMORY STATE (Railway has ephemeral filesystem) ───────
let state = {
  version: 'V1',
  startedAt: null,
  price: 0,
  change24h: 0,
  balance: INITIAL_BALANCE,
  equity: INITIAL_BALANCE,
  position: null,
  prices: [],
  equityHistory: [INITIAL_BALANCE],
  cooldown: 0,
  regime: 'NORMAL',
  wins: 0,
  losses: 0,
  bestTrade: 0,
  worstTrade: 0,
  posSize: 10,
  slPct: 0.02,
  tpPct: 0.04,
  running: false,
};
let trades = [];

// ─── PRICE FETCHER ────────────────────────────────────────────
async function fetchPrice() {
  const sources = [
    async () => {
      const r = await fetch('https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT', { timeout: 5000 });
      const d = await r.json();
      return { price: parseFloat(d.lastPrice), change: parseFloat(d.priceChangePercent) };
    },
    async () => {
      const r = await fetch('https://api.coinbase.com/v2/prices/BTC-USD/spot', { timeout: 5000 });
      const d = await r.json();
      return { price: parseFloat(d.data.amount), change: null };
    },
  ];
  for (const fn of sources) {
    try {
      const result = await fn();
      if (result.price && !isNaN(result.price)) return result;
    } catch (e) {}
  }
  return null;
}

// ─── BOT LOGIC ────────────────────────────────────────────────
function runBotTick(newPrice) {
  if (!state.running) return;
  if (!state.startedAt) state.startedAt = new Date().toISOString();

  state.price = newPrice;
  state.prices.push(newPrice);
  if (state.prices.length > MAX_PRICES) state.prices.shift();
  if (state.cooldown > 0) state.cooldown--;

  const sig = computeSignal(state.prices, state.cooldown);
  state.regime = sig.regime;
  if (sig.spike) state.cooldown = SPIKE_COOLDOWN;

  const effSize = state.balance * (state.posSize / 100) * sig.chaosFilter;

  if (state.position) {
    const p = state.position;
    const pnl    = p.type === 'long' ? (state.price - p.entry) * p.qty : (p.entry - state.price) * p.qty;
    const pnlPct = pnl / (p.entry * p.qty);
    const dynSL  = state.slPct * (sig.regime === 'EXTREME' ? 0.4 : sig.regime === 'VOLATILE' ? 0.6 : 1);
    const dynTP  = state.tpPct * (sig.regime === 'EXTREME' ? 0.5 : sig.regime === 'VOLATILE' ? 0.7 : 1);

    let closeReason = null;
    if      (pnlPct <= -dynSL) closeReason = 'SL';
    else if (pnlPct >=  dynTP) closeReason = 'TP';
    else if (sig.action !== 'wait' &&
      ((p.type === 'long'  && sig.action === 'short') ||
       (p.type === 'short' && sig.action === 'long'))) closeReason = 'REV';

    if (closeReason) closeTrade(pnl, closeReason, sig);
  }

  if (!state.position && sig.chaosFilter > 0 &&
      (sig.action === 'long' || sig.action === 'short') &&
      state.balance > 200 && effSize > 50) {
    state.position = {
      type:     sig.action,
      entry:    state.price,
      size:     effSize,
      qty:      effSize / state.price,
      openedAt: new Date().toISOString(),
    };
  }

  let unreal = 0;
  if (state.position) {
    unreal = state.position.type === 'long'
      ? (state.price - state.position.entry) * state.position.qty
      : (state.position.entry - state.price) * state.position.qty;
  }
  state.equity = state.balance + unreal;
  state.equityHistory.push(state.equity);
  if (state.equityHistory.length > 2000) state.equityHistory.shift();

  broadcast({ type: 'tick', state: publicState(), sig });
}

function closeTrade(pnl, reason, sig) {
  const p = state.position;
  state.balance += pnl;
  const trade = {
    id:       trades.length + 1,
    version:  'V1',
    type:     p.type,
    entry:    p.entry,
    exit:     state.price,
    size:     p.size,
    qty:      p.qty,
    pnl:      parseFloat(pnl.toFixed(4)),
    pnlPct:   parseFloat((pnl / p.size * 100).toFixed(3)),
    reason,
    regime:   sig.regime,
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
  console.log(`[TRADE] ${trade.type.toUpperCase()} | ${reason} | PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)} | Balance: $${state.balance.toFixed(2)}`);
  broadcast({ type: 'trade', trade });
}

function publicState() {
  return {
    ...state,
    equityHistory: state.equityHistory.slice(-300),
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
  const totalPnl = trades.reduce((s, t) => s + t.pnl, 0);
  const avgWin   = wins.length   ? wins.reduce((s,t)=>s+t.pnl,0)/wins.length   : 0;
  const avgLoss  = losses.length ? losses.reduce((s,t)=>s+t.pnl,0)/losses.length : 0;
  const pf = avgLoss !== 0 ? Math.abs(avgWin / avgLoss) : 999;

  const byRegime = {}, byReason = {};
  for (const t of trades) {
    if (!byRegime[t.regime]) byRegime[t.regime] = { count:0, pnl:0, wins:0 };
    byRegime[t.regime].count++; byRegime[t.regime].pnl += t.pnl;
    if (t.pnl > 0) byRegime[t.regime].wins++;
    if (!byReason[t.reason]) byReason[t.reason] = { count:0, pnl:0 };
    byReason[t.reason].count++; byReason[t.reason].pnl += t.pnl;
  }

  let peak = INITIAL_BALANCE, maxDD = 0, maxCL = 0, curCL = 0;
  for (const eq of state.equityHistory) {
    if (eq > peak) peak = eq;
    const dd = (peak - eq) / peak * 100;
    if (dd > maxDD) maxDD = dd;
  }
  for (const t of trades) {
    if (t.pnl < 0) { curCL++; maxCL = Math.max(maxCL, curCL); } else curCL = 0;
  }

  const longs  = trades.filter(t => t.type === 'long');
  const shorts = trades.filter(t => t.type === 'short');

  return {
    summary: {
      total, winRate: (wins.length/total*100).toFixed(1)+'%',
      totalPnl: +totalPnl.toFixed(2),
      totalPnlPct: +((totalPnl/INITIAL_BALANCE)*100).toFixed(2),
      avgWin: +avgWin.toFixed(2), avgLoss: +avgLoss.toFixed(2),
      profitFactor: +pf.toFixed(2),
      maxDrawdownPct: +maxDD.toFixed(2), maxConsecLoss: maxCL,
    },
    byRegime: Object.entries(byRegime).map(([k,v]) => ({
      regime: k, ...v, pnl: +v.pnl.toFixed(2), winRate: +(v.wins/v.count*100).toFixed(1),
    })),
    byReason: Object.entries(byReason).map(([k,v]) => ({
      reason: k, ...v, pnl: +v.pnl.toFixed(2),
    })),
    byDirection: {
      longs:  { count: longs.length,  pnl: +longs.reduce((s,t)=>s+t.pnl,0).toFixed(2) },
      shorts: { count: shorts.length, pnl: +shorts.reduce((s,t)=>s+t.pnl,0).toFixed(2) },
    },
    recent: trades.slice(-10),
  };
}

// ─── EXPRESS + WS ────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

app.get('/api/state',    (_, res) => res.json(publicState()));
app.get('/api/trades',   (_, res) => res.json(trades));
app.get('/api/analysis', (_, res) => res.json(analysePerformance()));

app.post('/api/start', (req, res) => {
  if (req.body.posSize) state.posSize = req.body.posSize;
  if (req.body.slPct)   state.slPct   = req.body.slPct;
  if (req.body.tpPct)   state.tpPct   = req.body.tpPct;
  state.running   = true;
  if (!state.startedAt) state.startedAt = new Date().toISOString();
  broadcast({ type: 'tick', state: publicState(), sig: {} });
  res.json({ ok: true });
  console.log('[APEX] Bot started');
});

app.post('/api/stop', (_, res) => {
  state.running = false;
  res.json({ ok: true });
  console.log('[APEX] Bot stopped');
});

app.post('/api/reset', (_, res) => {
  state.running = false; state.startedAt = null;
  state.balance = INITIAL_BALANCE; state.equity = INITIAL_BALANCE;
  state.position = null; state.prices = []; state.equityHistory = [INITIAL_BALANCE];
  state.cooldown = 0; state.regime = 'NORMAL';
  state.wins = 0; state.losses = 0; state.bestTrade = 0; state.worstTrade = 0;
  trades = [];
  broadcast({ type: 'reset' });
  res.json({ ok: true });
  console.log('[APEX] Bot reset');
});

function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
}

wss.on('connection', ws => {
  console.log('[WS] Client connected');
  ws.send(JSON.stringify({ type: 'init', state: publicState(), trades: trades.slice(-50) }));
});

// ─── PRICE LOOP ──────────────────────────────────────────────
async function priceLoop() {
  const result = await fetchPrice();
  if (result) {
    state.change24h = result.change ?? state.change24h;
    if (state.running) {
      runBotTick(result.price);
    } else {
      state.price = result.price;
      broadcast({ type: 'price', price: result.price, change24h: state.change24h });
    }
    console.log(`[PRICE] BTC $${result.price.toFixed(2)} | ${state.running ? 'BOT RUNNING' : 'standby'}`);
  } else {
    console.log('[PRICE] Fetch failed — skipping tick');
  }
}

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n╔══════════════════════════════════════╗`);
  console.log(`║   APEX BTC BOT V1 — Port ${PORT}        ║`);
  console.log(`╚══════════════════════════════════════╝\n`);
  priceLoop();
  setInterval(priceLoop, PRICE_INTERVAL);
});
