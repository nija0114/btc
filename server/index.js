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
const MAX_PRICES      = 1000;

// ─── STATE ───────────────────────────────────────────────────
let state = {
  version:'V1-RT', startedAt:null, price:0, prevPrice:0, change24h:0,
  balance:INITIAL_BALANCE, equity:INITIAL_BALANCE, position:null,
  prices:[], equityHistory:[INITIAL_BALANCE], cooldown:0, regime:'NORMAL',
  wins:0, losses:0, bestTrade:0, worstTrade:0,
  posSize:10, slPct:0.02, tpPct:0.04, running:false,
  tickCount:0, lastTick:null, wsConnected:false, priceSource:'connecting...',
};
let trades  = [];
let lastSig = { action:'wait', signals:[50,50,50,50,50], regime:'NORMAL', chaosFilter:1 };
let lastSampleTime=0, lastBotLogicTime=0, lastBroadcastTime=0;
let binanceWs=null, reconnectTimer=null, pollTimer=null, wsFailCount=0;

// ─── CORE TICK HANDLER ───────────────────────────────────────
function handlePriceTick(price) {
  if (!price || isNaN(price) || price <= 0) return;
  state.prevPrice = state.price;
  state.price     = price;
  state.tickCount++;
  state.lastTick  = new Date().toISOString();
  const now = Date.now();

  // Real-time SL/TP on every tick
  if (state.running && state.position) checkSLTP(price);

  // Sample every 1s for indicators
  if (now - lastSampleTime >= 1000) {
    lastSampleTime = now;
    state.prices.push(price);
    if (state.prices.length > MAX_PRICES) state.prices.shift();
  }

  // Strategy every 1s
  if (state.running && now - lastBotLogicTime >= 1000) {
    lastBotLogicTime = now;
    runBotLogic(price);
  }

  // Broadcast every 2s
  if (now - lastBroadcastTime >= 2000) {
    lastBroadcastTime = now;
    broadcast({ type:'tick', state:publicState(), sig:lastSig });
  }
}

// ─── PRICE SOURCES ───────────────────────────────────────────
function connectPriceStream() {
  clearTimeout(reconnectTimer);
  clearTimeout(pollTimer);

  // Binance is geo-blocked on Railway US servers after 2 attempts — use REST
  if (wsFailCount >= 2) {
    console.log('[PRICE] Switching to REST polling (geo-block detected)');
    startRestPolling();
    return;
  }

  if (binanceWs) { try { binanceWs.terminate(); } catch(e) {} binanceWs = null; }

  const wsUrl = 'wss://stream.binance.us:9443/ws/btcusdt@aggTrade';
  console.log(`[WS] Connecting Binance US (attempt ${wsFailCount+1})...`);

  try { binanceWs = new WebSocket(wsUrl); } catch(e) {
    wsFailCount++;
    reconnectTimer = setTimeout(connectPriceStream, 2000);
    return;
  }

  binanceWs.on('open', () => {
    console.log('[WS] ✓ Binance US connected');
    state.wsConnected = true;
    state.priceSource = 'Binance US WebSocket';
    wsFailCount = 0;
    fetch24hChange();
  });

  binanceWs.on('message', (raw) => {
    try { handlePriceTick(parseFloat(JSON.parse(raw).p)); } catch(e) {}
  });

  binanceWs.on('close', (code) => {
    state.wsConnected = false;
    console.log(`[WS] Closed (${code})`);
    if (code === 451 || code === 1006) wsFailCount++;
    reconnectTimer = setTimeout(connectPriceStream, 2000);
  });

  binanceWs.on('error', (err) => {
    state.wsConnected = false;
    console.log('[WS] Error:', err.message);
    if (err.message && (err.message.includes('451') || err.message.includes('403'))) wsFailCount = 99;
  });
}

async function startRestPolling() {
  clearTimeout(pollTimer);

  async function poll() {
    const sources = [
      async () => {
        const r = await fetch('https://api.kraken.com/0/public/Ticker?pair=XBTUSD', { timeout:4000 });
        const d = await r.json();
        const p = parseFloat(d.result?.XXBTZUSD?.c?.[0]);
        if (!p||isNaN(p)) throw new Error('no price');
        return { price:p, name:'Kraken' };
      },
      async () => {
        const r = await fetch('https://api.coinbase.com/v2/prices/BTC-USD/spot', { timeout:4000 });
        const d = await r.json();
        const p = parseFloat(d.data?.amount);
        if (!p||isNaN(p)) throw new Error('no price');
        return { price:p, name:'Coinbase' };
      },
      async () => {
        const r = await fetch('https://api.binance.us/api/v3/ticker/price?symbol=BTCUSDT', { timeout:4000 });
        const d = await r.json();
        const p = parseFloat(d.price);
        if (!p||isNaN(p)) throw new Error('no price');
        return { price:p, name:'Binance US REST' };
      },
    ];

    let ok = false;
    for (const fn of sources) {
      try {
        const { price, name } = await fn();
        if (state.priceSource !== name) { state.priceSource = name; console.log(`[REST] Source: ${name} — $${price.toFixed(2)}`); }
        state.wsConnected = true;
        handlePriceTick(price);
        ok = true;
        break;
      } catch(e) {}
    }
    if (!ok) { state.wsConnected = false; console.log('[REST] All sources failed'); }
    pollTimer = setTimeout(poll, 1000);
  }

  poll();
}

async function fetch24hChange() {
  try {
    const r = await fetch('https://api.kraken.com/0/public/Ticker?pair=XBTUSD', { timeout:4000 });
    const d = await r.json();
    const t = d.result?.XXBTZUSD;
    const open = parseFloat(t?.o), last = parseFloat(t?.c?.[0]);
    state.change24h = (last - open) / open * 100;
    console.log(`[24H] ${state.change24h.toFixed(2)}%`);
  } catch(e) {
    try {
      const r = await fetch('https://api.binance.us/api/v3/ticker/24hr?symbol=BTCUSDT', { timeout:4000 });
      const d = await r.json();
      state.change24h = parseFloat(d.priceChangePercent);
    } catch(e2) {}
  }
}
setInterval(fetch24hChange, 3600000);

// ─── SL/TP ───────────────────────────────────────────────────
function checkSLTP(price) {
  if (!state.position) return;
  const p = state.position;
  const pnl    = p.type==='long' ? (price-p.entry)*p.qty : (p.entry-price)*p.qty;
  const pnlPct = pnl / (p.entry*p.qty);
  const dynSL  = state.slPct * (state.regime==='EXTREME'?0.4:state.regime==='VOLATILE'?0.6:1);
  const dynTP  = state.tpPct * (state.regime==='EXTREME'?0.5:state.regime==='VOLATILE'?0.7:1);
  if      (pnlPct <= -dynSL) closeTrade(pnl,'SL',lastSig);
  else if (pnlPct >=  dynTP) closeTrade(pnl,'TP',lastSig);
}

// ─── BOT LOGIC ───────────────────────────────────────────────
function runBotLogic(price) {
  if (state.prices.length < 30) return;
  if (state.cooldown > 0) state.cooldown--;
  const sig    = computeSignal(state.prices, state.cooldown);
  lastSig      = sig;
  state.regime = sig.regime;
  if (sig.spike) { state.cooldown=SPIKE_COOLDOWN; console.log(`[SPIKE] @ $${price.toFixed(2)}`); }

  if (state.position) {
    const p = state.position;
    if (sig.action!=='wait' && ((p.type==='long'&&sig.action==='short')||(p.type==='short'&&sig.action==='long'))) {
      closeTrade(p.type==='long'?(price-p.entry)*p.qty:(p.entry-price)*p.qty, 'REV', sig);
    }
  }

  const effSize = state.balance*(state.posSize/100)*sig.chaosFilter;
  if (!state.position && sig.chaosFilter>0 && (sig.action==='long'||sig.action==='short') && state.balance>200 && effSize>50) {
    state.position = { type:sig.action, entry:price, size:effSize, qty:effSize/price, openedAt:new Date().toISOString() };
    console.log(`[OPEN] ${sig.action.toUpperCase()} @ $${price.toFixed(2)} | $${effSize.toFixed(0)} | ${sig.regime}`);
  }

  let unreal = 0;
  if (state.position) unreal = state.position.type==='long' ? (price-state.position.entry)*state.position.qty : (state.position.entry-price)*state.position.qty;
  state.equity = state.balance + unreal;
  state.equityHistory.push(state.equity);
  if (state.equityHistory.length > 86400) state.equityHistory.shift();
}

function closeTrade(pnl, reason, sig) {
  if (!state.position) return;
  const p = state.position;
  state.balance += pnl;
  const trade = {
    id:trades.length+1, version:'V1-RT', type:p.type,
    entry:p.entry, exit:state.price, size:p.size, qty:p.qty,
    pnl:+pnl.toFixed(4), pnlPct:+(pnl/p.size*100).toFixed(3),
    reason, regime:sig.regime||state.regime,
    rsi:sig.rsi?+sig.rsi.toFixed(2):null,
    openedAt:p.openedAt, closedAt:new Date().toISOString(),
    balance:+state.balance.toFixed(2),
  };
  trades.push(trade);
  pnl>0?state.wins++:state.losses++;
  if(pnl>state.bestTrade) state.bestTrade=pnl;
  if(pnl<state.worstTrade) state.worstTrade=pnl;
  state.position=null;
  console.log(`[CLOSE] ${trade.type.toUpperCase()} | ${reason} | ${pnl>=0?'+':''}$${pnl.toFixed(2)} | Bal:$${state.balance.toFixed(2)}`);
  broadcast({ type:'trade', trade });
}

function publicState() {
  return { ...state, equityHistory:state.equityHistory.slice(-500), prices:state.prices.slice(-200), tradeCount:trades.length };
}

function analysePerformance() {
  if (!trades.length) return { error:'No trades yet' };
  const total=trades.length, wins=trades.filter(t=>t.pnl>0), losses=trades.filter(t=>t.pnl<0);
  const totalPnl=trades.reduce((s,t)=>s+t.pnl,0);
  const avgWin=wins.length?wins.reduce((s,t)=>s+t.pnl,0)/wins.length:0;
  const avgLoss=losses.length?losses.reduce((s,t)=>s+t.pnl,0)/losses.length:0;
  const pf=avgLoss?Math.abs(avgWin/avgLoss):999;
  const byRegime={},byReason={};
  for(const t of trades){
    if(!byRegime[t.regime])byRegime[t.regime]={count:0,pnl:0,wins:0};
    byRegime[t.regime].count++;byRegime[t.regime].pnl+=t.pnl;if(t.pnl>0)byRegime[t.regime].wins++;
    if(!byReason[t.reason])byReason[t.reason]={count:0,pnl:0};
    byReason[t.reason].count++;byReason[t.reason].pnl+=t.pnl;
  }
  let peak=INITIAL_BALANCE,maxDD=0,maxCL=0,curCL=0;
  for(const eq of state.equityHistory){if(eq>peak)peak=eq;const dd=(peak-eq)/peak*100;if(dd>maxDD)maxDD=dd;}
  for(const t of trades){if(t.pnl<0){curCL++;maxCL=Math.max(maxCL,curCL);}else curCL=0;}
  const longs=trades.filter(t=>t.type==='long'),shorts=trades.filter(t=>t.type==='short');
  const durs=trades.map(t=>(new Date(t.closedAt)-new Date(t.openedAt))/60000);
  return {
    summary:{total,winRate:(wins.length/total*100).toFixed(1)+'%',totalPnl:+totalPnl.toFixed(2),
      totalPnlPct:+((totalPnl/INITIAL_BALANCE)*100).toFixed(2),avgWin:+avgWin.toFixed(2),
      avgLoss:+avgLoss.toFixed(2),profitFactor:+pf.toFixed(2),maxDrawdownPct:+maxDD.toFixed(2),
      maxConsecLoss:maxCL,avgTradeDurationMins:+(durs.reduce((a,b)=>a+b,0)/durs.length).toFixed(1),
      totalTicksReceived:state.tickCount,priceSource:state.priceSource},
    byRegime:Object.entries(byRegime).map(([k,v])=>({regime:k,...v,pnl:+v.pnl.toFixed(2),winRate:+(v.wins/v.count*100).toFixed(1)})),
    byReason:Object.entries(byReason).map(([k,v])=>({reason:k,...v,pnl:+v.pnl.toFixed(2)})),
    byDirection:{longs:{count:longs.length,pnl:+longs.reduce((s,t)=>s+t.pnl,0).toFixed(2)},shorts:{count:shorts.length,pnl:+shorts.reduce((s,t)=>s+t.pnl,0).toFixed(2)}},
    recent:trades.slice(-10),allTrades:trades,
  };
}

// ─── EXPRESS + WS ─────────────────────────────────────────────
const app=express(), server=http.createServer(app), wss=new WebSocket.Server({server});
app.use(cors()); app.use(express.json()); app.use(express.static(path.join(__dirname,'../public')));
app.get('/api/state',    (_,res)=>res.json(publicState()));
app.get('/api/trades',   (_,res)=>res.json(trades));
app.get('/api/analysis', (_,res)=>res.json(analysePerformance()));
app.get('/api/health',   (_,res)=>res.json({ok:true,wsConnected:state.wsConnected,tickCount:state.tickCount,price:state.price,priceSource:state.priceSource,running:state.running}));
app.post('/api/start',(req,res)=>{
  if(req.body.posSize)state.posSize=req.body.posSize;
  if(req.body.slPct)state.slPct=req.body.slPct;
  if(req.body.tpPct)state.tpPct=req.body.tpPct;
  state.running=true;if(!state.startedAt)state.startedAt=new Date().toISOString();
  res.json({ok:true});broadcast({type:'tick',state:publicState(),sig:lastSig});
  console.log(`[APEX] Started — SL:${(state.slPct*100).toFixed(1)}% TP:${(state.tpPct*100).toFixed(1)}% Size:${state.posSize}%`);
});
app.post('/api/stop',(_,res)=>{state.running=false;res.json({ok:true});});
app.post('/api/reset',(_,res)=>{
  state.running=false;state.startedAt=null;state.balance=INITIAL_BALANCE;state.equity=INITIAL_BALANCE;
  state.position=null;state.prices=[];state.equityHistory=[INITIAL_BALANCE];state.cooldown=0;state.regime='NORMAL';
  state.wins=0;state.losses=0;state.bestTrade=0;state.worstTrade=0;state.tickCount=0;state.lastTick=null;
  trades=[];broadcast({type:'reset'});res.json({ok:true});
});
function broadcast(data){const m=JSON.stringify(data);wss.clients.forEach(c=>{if(c.readyState===WebSocket.OPEN)c.send(m);});}
wss.on('connection',ws=>{ws.send(JSON.stringify({type:'init',state:publicState(),trades:trades.slice(-50)}));});

server.listen(PORT,'0.0.0.0',()=>{
  console.log(`\n APEX BTC BOT V1-RT — Port ${PORT}\n`);
  wsFailCount = 99; // skip WS, go straight to REST (Railway geo-blocks Binance)
  connectPriceStream();
});
