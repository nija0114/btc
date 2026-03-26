# APEX BTC BOT — V1
## Bidirectional Live Paper Trading Bot

---

## QUICK START (local)

```bash
cd apex-btc-bot
npm install
npm start
```
Open: http://localhost:3000

---

## DEPLOY TO A SERVER

### Option A — Railway (free, recommended)
1. Push this folder to a GitHub repo
2. Go to https://railway.app → New Project → Deploy from GitHub
3. Railway auto-detects Node.js and runs `npm start`
4. Get your public URL (e.g. `https://apex-btc-bot.up.railway.app`)

### Option B — Render.com (free tier)
1. Push to GitHub
2. https://render.com → New Web Service → connect repo
3. Build command: `npm install`
4. Start command: `npm start`
5. Free tier spins down after inactivity — use paid ($7/mo) for 24h uptime

### Option C — VPS (DigitalOcean/Linode $4/mo)
```bash
git clone <your-repo>
cd apex-btc-bot
npm install
npm install -g pm2
pm2 start server/index.js --name apex-bot
pm2 save
pm2 startup
```

---

## HOW IT WORKS

- Server fetches **live BTC/USD from Binance** every 10 seconds
- Bot runs V1 strategy on each tick (RSI + MACD + BB + EMA cross)
- **Regime detection** blocks trades during volatile/extreme markets
- All trades saved to `data/trades.json` — survives server restarts
- Dashboard available at `/` with live WebSocket updates

---

## API ENDPOINTS

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/state` | Full bot state |
| GET | `/api/trades` | All trade history |
| GET | `/api/analysis` | Performance analysis + insights |
| POST | `/api/start` | Start bot (body: `{posSize, slPct, tpPct}`) |
| POST | `/api/stop` | Stop bot |
| POST | `/api/reset` | Reset all trades + balance |

---

## AFTER 24 HOURS

Share the `/api/analysis` JSON output with Claude (paste it in chat).
Claude will review the trade breakdown — win rate by regime, exit reasons,
drawdown — and produce **V2** with targeted fixes.

---

## STRATEGY — V1 RULES

**NORMAL regime:**
- Signal = weighted RSI(28%) + MACD(22%) + BB(25%) + EMA cross(25%)
- Threshold: score > 63 to open
- Full position size

**VOLATILE regime** (short ATR > 1.5× medium ATR):
- Signal = RSI(55%) + BB(45%) — mean reversion only
- Threshold: > 82
- 25% position size, SL tightened 40%

**EXTREME regime** (spike OR short ATR > 2.5× medium):
- ZERO new trades
- 18-tick cooldown after spike
- SL tightened 60% on any open position

---

*APEX BTC BOT V1 — Paper trading only. Not financial advice.*
