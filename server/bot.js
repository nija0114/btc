'use strict';

// ─── CONSTANTS ────────────────────────────────────────────────
const SPIKE_COOLDOWN = 10; // reduced from 18 — real markets recover faster

// ─── INDICATORS ──────────────────────────────────────────────
function calcEMA(prices, period) {
  if (!prices || prices.length < 2) return null;
  const k = 2 / (period + 1);
  const start = Math.min(period, prices.length);
  let e = prices.slice(0, start).reduce((a, b) => a + b) / start;
  for (let i = start; i < prices.length; i++) e = prices[i] * k + e * (1 - k);
  return e;
}

function calcRSI(prices, period = 14) {
  if (prices.length < period + 1) return 50;
  const sl = prices.slice(-(period + 1));
  const ch = sl.map((p, i) => i > 0 ? p - sl[i - 1] : 0).slice(1);
  const ag = ch.map(c => c > 0 ? c : 0).reduce((a, b) => a + b) / period;
  const al = ch.map(c => c < 0 ? -c : 0).reduce((a, b) => a + b) / period;
  return al === 0 ? 100 : 100 - 100 / (1 + ag / al);
}

function calcMACD(prices) {
  if (prices.length < 26) return { hist: 0 };
  const h = calcEMA(prices, 12) - calcEMA(prices, 26);
  return { hist: h - h * 0.88 };
}

function calcBB(prices, period = 20) {
  if (prices.length < period) return { upper: 0, lower: 0, mid: 0, pct: 50 };
  const sl = prices.slice(-period);
  const mid = sl.reduce((a, b) => a + b) / period;
  const std = Math.sqrt(sl.map(p => (p - mid) ** 2).reduce((a, b) => a + b) / period);
  const [up, lo] = [mid + 2 * std, mid - 2 * std];
  const p = prices[prices.length - 1];
  return { upper: up, lower: lo, mid, pct: (up - lo) > 0 ? (p - lo) / (up - lo) * 100 : 50 };
}

function calcATRpct(prices, period = 14) {
  if (prices.length < 2) return 0;
  const sl = prices.slice(-(period + 1));
  let s = 0;
  for (let i = 1; i < sl.length; i++) s += Math.abs(sl[i] - sl[i - 1]);
  return (s / (sl.length - 1)) / prices[prices.length - 1] * 100;
}

// ─── REGIME DETECTION ────────────────────────────────────────
// RECALIBRATED for real 1-second BTC price ticks from Kraken/Coinbase.
// Real BTC on 1s ticks has naturally higher ATR than simulation.
// Old thresholds (0.22 / 0.5) were too tight — flagging normal moves as EXTREME.
//
// Calibration based on typical BTC 1s tick behavior:
//   Normal quiet session:  atrShort ~0.01–0.04%
//   Normal active session: atrShort ~0.04–0.10%
//   Volatile (news/pump):  atrShort ~0.10–0.25%
//   Extreme (flash crash): atrShort > 0.25%, ratio > 3x
//
// Also: spike detection threshold raised to 8x (was 4x) because
// individual 1s ticks naturally vary more than 4x the baseline.

function detectRegime(prices) {
  if (prices.length < 15) return { regime: 'NORMAL', atr: 0, spike: false };

  const atrShort  = calcATRpct(prices, 5);   // last 5 seconds
  const atrMedium = calcATRpct(prices, 30);  // last 30 seconds baseline
  const ratio     = atrMedium > 0 ? atrShort / atrMedium : 1;

  // Spike: last single move is 8x the recent average (was 4x — too sensitive)
  let spike = false;
  if (prices.length >= 3) {
    const lastMove = Math.abs(prices[prices.length - 1] - prices[prices.length - 2])
                     / prices[prices.length - 2] * 100;
    spike = lastMove > atrMedium * 8;
  }

  // EXTREME: only genuine flash crashes / massive spikes
  // atrShort > 0.25% (was 0.5% of price per second — that's enormous)
  // ratio > 4x (was 2.5x — too easily triggered)
  if (spike || ratio > 4.0 || atrShort > 0.25)  return { regime: 'EXTREME',  atr: atrShort, spike };

  // VOLATILE: elevated but tradeable
  // atrShort > 0.10% or ratio > 2.5x
  if (ratio > 2.5 || atrShort > 0.10)           return { regime: 'VOLATILE', atr: atrShort, spike };

  return { regime: 'NORMAL', atr: atrShort, spike };
}

// ─── SIGNAL ──────────────────────────────────────────────────
function computeSignal(prices, cooldown) {
  if (prices.length < 30) {
    return { action: 'wait', signals: [50,50,50,50,50], regime: 'NORMAL', chaosFilter: 1 };
  }

  const { regime, atr, spike } = detectRegime(prices);
  const s5 = regime === 'EXTREME' ? 4 : regime === 'VOLATILE' ? 35 : 100;

  // EXTREME or post-spike cooldown → no new trades
  if (regime === 'EXTREME' || cooldown > 0) {
    return {
      action: 'wait',
      rsi: calcRSI(prices), hist: 0,
      bb: calcBB(prices),
      ema9: calcEMA(prices, 9), ema21: calcEMA(prices, 21),
      atr, regime, chaosFilter: 0, spike,
      signals: [50, 50, 50, 50, s5]
    };
  }

  const R     = calcRSI(prices);
  const M     = calcMACD(prices).hist;
  const B     = calcBB(prices);
  const e9    = calcEMA(prices, 9);
  const e21   = calcEMA(prices, 21);
  const price = prices[prices.length - 1];

  // RSI signal
  let s1 = 50;
  if      (R < 25) s1 = 92; else if (R < 32) s1 = 70;
  else if (R > 75) s1 =  8; else if (R > 68) s1 = 30;

  // MACD histogram
  const s2 = Math.max(0, Math.min(100, 50 + M / price * 80000));

  // BB %B
  let s3 = 50;
  if      (B.pct <  6) s3 = 88; else if (B.pct < 18) s3 = 66;
  else if (B.pct > 94) s3 = 12; else if (B.pct > 82) s3 = 34;

  // EMA cross
  const gap = e9 && e21 ? (e9 - e21) / e21 * 100 : 0;
  const s4  = Math.max(10, Math.min(90, 50 + gap * 12));

  // VOLATILE: mean-reversion only, 25% size
  const longScore = regime === 'VOLATILE'
    ? s1 * 0.55 + s3 * 0.45
    : s1 * 0.28 + s2 * 0.22 + s3 * 0.25 + s4 * 0.25;
  const shortScore = 100 - longScore;

  const thresh      = regime === 'VOLATILE' ? 80 : 63;
  const chaosFilter = regime === 'VOLATILE' ? 0.25 : 1.0;

  let action = 'wait';
  if      (longScore  > thresh) action = 'long';
  else if (shortScore > thresh) action = 'short';

  return {
    action, rsi: R, hist: M, bb: B, ema9: e9, ema21: e21,
    atr, regime, chaosFilter, spike,
    signals: [s1, Math.max(0,Math.min(100,s2)), s3, Math.max(0,Math.min(100,s4)), s5]
  };
}

module.exports = { computeSignal, detectRegime, calcRSI, calcBB, calcEMA, calcATRpct, SPIKE_COOLDOWN };
