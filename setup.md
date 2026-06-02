# XAUUSD Signal Alerter — Setup Guide

A FastAPI service that monitors XAU/USD on the 5-minute timeframe, runs the
EMA Pullback strategy (Strategy 1 from the Pine indicator), and emails you
when a trade signal forms and when it resolves (TP or SL hit).

- Polls Twelve Data every 5 minutes, ~15s after each bar closes
- Maintains EMA50, EMA200, ATR14 with Pine-faithful math
- One trade at a time — new signals are ignored while a trade is open
- Persists state to `state.json` so trades survive restarts
- Handles weekend market closure, API outages, and missed-bar gap fills
- Tested target: Python 3.10.7

---

## 1. Requirements

- Python **3.10.7** (the only version this is pinned for)
- A Gmail account with **2-Step Verification ON** and an **App Password**
- A Twelve Data API key (free tier is sufficient — uses ~290 of 800 daily credits)

---

## 2. Installation

```bash
unzip xau-alerter.zip
cd xau-alerter

# Create venv (using Python 3.10.7)
python3.10 -m venv venv

# Activate venv
source venv/bin/activate          # Linux / macOS
# OR
venv\Scripts\activate             # Windows

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 3. Configuration

```bash
cp .env.example .env
nano .env                          # or any editor
```

Fill in these required fields:

| Variable | What to put |
|---|---|
| `TWELVEDATA_API_KEY` | Your Twelve Data API key from https://twelvedata.com |
| `SMTP_USER` | Your Gmail address (sender) |
| `SMTP_PASS` | Your Gmail **App Password** (16 chars, no spaces) — NOT your normal password |
| `EMAIL_FROM` | Same Gmail address as `SMTP_USER` |
| `EMAIL_TO` | The address that should receive alerts |
| `DISPLAY_TIMEZONE` | `Asia/Kolkata` (default) or any IANA tz name |

### How to generate a Gmail App Password

1. Go to https://myaccount.google.com/security and ensure **2-Step Verification** is ON.
2. Go to https://myaccount.google.com/apppasswords.
3. Pick "Mail" and "Other (custom name)" → name it `xau-alerter`.
4. Google shows a 16-character password — paste it into `.env` as `SMTP_PASS` (remove spaces).

---

## 4. First run

```bash
source venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000
```

On startup the service will:

1. Fetch ~600 historical 5-min bars (1 API credit)
2. Warm up EMA50, EMA200, ATR14
3. Wait for the next 5-min bar close + 15s buffer
4. Poll every 5 minutes thereafter

You should see log lines like:

```
Worker loop started
Cold start: fetching 600 historical 5-min bars...
Indicators warmed. Latest bar=2026-06-02 21:30 UTC  close=4530.685  ema50=...
```

### Test the email pipeline

In another terminal (or from your phone over the network):

```bash
curl -X POST http://localhost:8000/test-email
```

You should receive an email titled "Test email from xau-alerter" within a few seconds. If you don't, check the service logs — common issues:

- Gmail App Password has spaces in it (remove them)
- SMTP_USER and EMAIL_FROM don't match
- Firewall blocking outbound port 587

---

## 5. Useful endpoints

While the service is running, you can hit:

| Endpoint | Purpose |
|---|---|
| `GET /` | Basic info |
| `GET /health` | Quick status: last bar, in-trade, market-open flag |
| `GET /state` | Full internal state (indicators, trade, stats) — useful for debugging |
| `POST /test-email` | Sends a test email |

---

## 6. Running on the VPS

Pick whatever you prefer:

### Option A — systemd (recommended for production)

Create `/etc/systemd/system/xau-alerter.service`:

```ini
[Unit]
Description=XAUUSD Signal Alerter
After=network-online.target

[Service]
Type=simple
User=your_vps_user
WorkingDirectory=/home/your_vps_user/xau-alerter
ExecStart=/home/your_vps_user/xau-alerter/venv/bin/uvicorn main:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now xau-alerter
sudo journalctl -u xau-alerter -f      # live logs
```

### Option B — tmux

```bash
tmux new -s xau
source venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000
# detach: Ctrl-b then d
# reattach: tmux a -t xau
```

---

## 7. How the strategy works (TL;DR)

On every confirmed 5-min bar:

**Indicator update** (recursive — needs only previous EMA/ATR values):
- `EMA50  = α₅₀ × close + (1-α₅₀) × prev_EMA50`,  α₅₀ = 2/51
- `EMA200 = α₂₀₀× close + (1-α₂₀₀)× prev_EMA200`, α₂₀₀ = 2/201
- `ATR14  = (prev_ATR × 13 + TR) / 14`  (Wilder's RMA)

**Exit check (only if a trade is currently open):**
- BUY:  `TP_hit = high ≥ TP`,  `SL_hit = low ≤ SL`
- SELL: `TP_hit = low ≤ TP`,   `SL_hit = high ≥ SL`
- If both hit on the same bar → SL wins (Conservative)
- Gap exit: if bar opened past the level, exit at the open

**Entry check (only if no trade is open):**
- BUY  when `close > EMA200` AND `low ≤ EMA50` AND `close > EMA50` AND `close > open`
- SELL when `close < EMA200` AND `high ≥ EMA50` AND `close < EMA50` AND `close < open`
- Entry = close of signal bar
- SL = entry ± 1.5 × ATR
- TP = entry ± 2 × (entry − SL)   (fixed 1:2 RR)

The entry bar is **never** checked for TP/SL — exits start from the next bar onward. This matches the Pine indicator behavior exactly.

---

## 8. Edge cases the service handles

| Situation | What happens |
|---|---|
| Network blip / API 5xx | One retry after 30s, then skip cycle |
| Service down for N bars (N ≤ 1000) | On recovery, fetches missed bars and replays the state machine — signals during downtime DO trigger alerts |
| Service down for > 1000 bars (≈ 3.5 days) | Treats as cold start, silently rebuilds indicators, does NOT spam old signals |
| Weekend (Fri 22:00 UTC → Sun 22:00 UTC) | Sleeps through, zero API calls. Refreshes indicators on Sunday reopen |
| Trade open at restart | Reloaded from `state.json`. New entries blocked until current trade resolves |
| Server clock not UTC | All internal logic uses UTC; only display strings use `DISPLAY_TIMEZONE` |
| Twelve Data daily credit limit | Steady state uses ~290/800 credits/day — plenty of headroom |

---

## 9. Future enhancement: time-window filter

To only get alerts during specific local hours (e.g. when you're awake to act on them), set in `.env`:

```
ALERT_TIME_WINDOWS=14:00-15:00,18:00-19:00
```

Times are in `DISPLAY_TIMEZONE` (IST by default). This only affects **entry** alerts; TP/SL outcome alerts always fire because they need to be reported regardless of when the trade resolves.

Restart the service after changing `.env`.

---

## 10. Troubleshooting

**No alerts after hours of running**
- Check `GET /health` — `in_trade` should match what you expect.
- Check `GET /state` — look at `indicator.last_bar_time`, should be within the last ~10 min during market hours.
- Tail logs: `journalctl -u xau-alerter -f` (systemd) or look at uvicorn's stdout.
- Signals genuinely don't fire often on EMA pullback — expect a few per day at most, sometimes zero on quiet days.

**Lots of "Fetch failed" log lines**
- Check Twelve Data dashboard for your credit usage at https://twelvedata.com/account
- The free tier is 800 credits/day; this service uses ~290. If exhausted, fetches return 429.

**Wrong timestamps in emails**
- Verify `DISPLAY_TIMEZONE` is a valid IANA tz name (e.g. `Asia/Kolkata`, `Europe/London`, `America/New_York`).
- All bar timestamps are shown in UTC; only "Alerted at" wall-clock time uses `DISPLAY_TIMEZONE`.