"""
XAUUSD 5-min EMA Pullback Signal Service
=========================================

A FastAPI service that polls Twelve Data every 5 minutes for confirmed
XAU/USD 5-min bars, computes EMA50 / EMA200 / ATR14 the same way the Pine
Script indicator does, evaluates the EMA Pullback strategy, and emails
signal + outcome alerts.

Single-file design. State persists in state.json so trade state survives
restarts. Indicator state is rebuilt from market data on every start
(market is always source-of-truth for indicators).
"""

from __future__ import annotations
import os
os.environ["TZ"] = "UTC"

import asyncio
import json
import logging
import smtplib
import sys
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI

# =============================================================================
# CONFIG
# =============================================================================

load_dotenv()


def _env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and not val:
        print(f"FATAL: required env var {name} is not set", file=sys.stderr)
        sys.exit(1)
    return val or ""


# --- API ---
TWELVEDATA_API_KEY = _env("TWELVEDATA_API_KEY", required=True)
SYMBOL = _env("SYMBOL", "XAU/USD")
INTERVAL = "5min"
INTERVAL_MIN = 5

# --- Strategy 1 (EMA Pullback) — matches Pine defaults ---
EMA_FAST_LEN = 50
EMA_SLOW_LEN = 200
ATR_LEN = 14
ATR_MULT = 1.5
RR_RATIO = 2.0  # fixed 1:2

# --- Loop / fetch behavior ---
BACKFILL_BARS = 600  # enough to fully warm EMA200 and ATR14
GAP_FILL_THRESHOLD = 1000  # if gap > this many bars, treat as cold start
POST_CLOSE_BUFFER_SEC = 15  # wait this long after bar close before fetching
MAX_FETCH_RETRIES = 1  # retry count on fetch failure (so 1 retry = 2 total attempts)
RETRY_SLEEP_SEC = 30
LOOP_EXCEPTION_SLEEP_SEC = 60

# --- Persistence ---
STATE_FILE = Path(_env("STATE_FILE", "state.json"))

# --- Email (Gmail SMTP by default) ---
SMTP_HOST = _env("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(_env("SMTP_PORT", "587"))
SMTP_USER = _env("SMTP_USER", required=True)
SMTP_PASS = _env("SMTP_PASS", required=True)
EMAIL_FROM = _env("EMAIL_FROM", SMTP_USER)
EMAIL_TO = _env("EMAIL_TO", required=True)

# --- Display ---
DISPLAY_TIMEZONE = _env("DISPLAY_TIMEZONE", "Asia/Kolkata")
SYMBOL_LABEL = _env("SYMBOL_LABEL", "XAUUSD")
STRATEGY_LABEL = "EMA Pullback (Strategy 1)"

# --- Future enhancement hook: alert-time windows in DISPLAY_TIMEZONE ---
# Example: "14:00-15:00,18:00-19:00"   (empty = always allow)
ALERT_TIME_WINDOWS = _env("ALERT_TIME_WINDOWS", "").strip()


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("xau-alerter")


# =============================================================================
# TIME HELPERS — all internal time is UTC
# =============================================================================


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_td_time(ts: str) -> datetime:
    """Parse a Twelve Data timestamp (we always request timezone=UTC)."""
    dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    assert dt.tzinfo is timezone.utc  # guaranteed by our timezone=UTC param
    return dt


def fmt_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fmt_display(dt: datetime) -> str:
    """Format a UTC datetime for display in the configured display timezone."""
    local = dt.astimezone(ZoneInfo(DISPLAY_TIMEZONE))
    return local.strftime("%d %b %Y, %H:%M:%S %Z")


def next_bar_close_utc(now: datetime) -> datetime:
    """Return the UTC time of the next 5-min bar CLOSE after `now`."""
    # A bar opens at minute % 5 == 0 and closes 5 minutes later.
    floor = now.replace(second=0, microsecond=0)
    floor = floor - timedelta(minutes=floor.minute % INTERVAL_MIN)
    next_close = floor + timedelta(minutes=INTERVAL_MIN)
    if next_close <= now:
        next_close += timedelta(minutes=INTERVAL_MIN)
    return next_close


def is_market_closed(now_utc: datetime) -> bool:
    """
    Gold market closed approximately Fri 22:00 UTC -> Sun 22:00 UTC.
    Conservative window — accepts a little extra closed time to avoid edge cases.
    """
    weekday = now_utc.weekday()  # Mon=0 ... Sun=6
    hour = now_utc.hour
    if weekday == 5:  # Saturday — all day
        return True
    if weekday == 4 and hour >= 22:  # Friday from 22:00 UTC
        return True
    if weekday == 6 and hour < 22:  # Sunday before 22:00 UTC
        return True
    return False


def next_market_open_utc(now_utc: datetime) -> datetime:
    """Next Sunday 22:00 UTC at or after `now_utc`."""
    # Walk forward day by day until we hit Sunday 22:00 UTC.
    candidate = now_utc.replace(hour=22, minute=0, second=0, microsecond=0)
    # Move to current/next Sunday at 22:00 UTC.
    while candidate.weekday() != 6 or candidate <= now_utc:
        candidate += timedelta(days=1)
        candidate = candidate.replace(hour=22, minute=0, second=0, microsecond=0)
    return candidate


def is_alert_time_allowed(bar_time_utc: datetime) -> bool:
    """
    Future enhancement hook: limit when alerts may fire based on local time
    windows specified in ALERT_TIME_WINDOWS (in DISPLAY_TIMEZONE).
    Empty config => always allowed.
    """
    if not ALERT_TIME_WINDOWS:
        return True
    local = bar_time_utc.astimezone(ZoneInfo(DISPLAY_TIMEZONE)).time()
    for window in ALERT_TIME_WINDOWS.split(","):
        window = window.strip()
        if not window:
            continue
        try:
            start_str, end_str = window.split("-")
            start_t = datetime.strptime(start_str.strip(), "%H:%M").time()
            end_t = datetime.strptime(end_str.strip(), "%H:%M").time()
        except ValueError:
            log.warning("Bad ALERT_TIME_WINDOWS entry: %r — ignored", window)
            continue
        # Handle windows that don't wrap midnight (we don't support wrap-around)
        if start_t <= local < end_t:
            return True
    return False


# =============================================================================
# INDICATOR MATH — Pine-faithful implementations
# =============================================================================
#
# Pine's `ta.ema(src, len)`:
#   alpha = 2 / (len + 1)
#   ema[0] = src[0]
#   ema[i] = alpha * src[i] + (1 - alpha) * ema[i-1]
#
# Pine's `ta.atr(len)` uses `ta.rma` on True Range:
#   TR[i] = max(high-low, |high - prev_close|, |low - prev_close|)
#   rma[i] = (rma[i-1] * (len-1) + TR[i]) / len    for i >= len
#   rma[len-1] = SMA(TR, len)                       (seed at bar `len-1`)
# =============================================================================


def ema_alpha(length: int) -> float:
    return 2.0 / (length + 1.0)


def seed_ema_series(closes: List[float], length: int) -> List[float]:
    """Compute full EMA series from a list of closes. Pine convention: ema[0]=closes[0]."""
    if not closes:
        return []
    alpha = ema_alpha(length)
    out = [closes[0]]
    for c in closes[1:]:
        out.append(alpha * c + (1.0 - alpha) * out[-1])
    return out


def seed_atr_series(
    highs: List[float], lows: List[float], closes: List[float], length: int
) -> List[Optional[float]]:
    """Compute full ATR (RMA of TR) series. Returns None for first length-1 bars."""
    n = len(closes)
    if n == 0:
        return []
    # Compute TR values
    trs: List[float] = []
    for i in range(n):
        if i == 0:
            trs.append(highs[i] - lows[i])
        else:
            prev_close = closes[i - 1]
            trs.append(
                max(
                    highs[i] - lows[i],
                    abs(highs[i] - prev_close),
                    abs(lows[i] - prev_close),
                )
            )
    # RMA seed = SMA of first `length` TRs, at index length-1
    atrs: List[Optional[float]] = [None] * n
    if n < length:
        return atrs
    seed = sum(trs[:length]) / length
    atrs[length - 1] = seed
    for i in range(length, n):
        prev = atrs[i - 1]
        # prev cannot be None here
        atrs[i] = (prev * (length - 1) + trs[i]) / length  # type: ignore[operator]
    return atrs


def step_ema(prev_ema: float, new_close: float, length: int) -> float:
    """Advance EMA by one bar."""
    a = ema_alpha(length)
    return a * new_close + (1.0 - a) * prev_ema


def step_atr(prev_atr: float, high: float, low: float, prev_close: float, length: int) -> float:
    """Advance ATR (RMA of TR) by one bar."""
    tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
    return (prev_atr * (length - 1) + tr) / length


# =============================================================================
# STATE
# =============================================================================
#
# state.json schema:
#   {
#     "indicator": {
#       "last_bar_time": "2026-06-02T21:30:00+00:00" | None,
#       "prev_close":    float                       | None,
#       "ema50":         float                       | None,
#       "ema200":        float                       | None,
#       "atr14":         float                       | None,
#       "bars_processed": int
#     },
#     "trade": {
#       "active":         bool,
#       "direction":      "BUY"|"SELL"|None,
#       "entry":          float|None,
#       "sl":             float|None,
#       "tp":             float|None,
#       "entry_bar_time": ISO8601 UTC | None
#     },
#     "stats": {
#       "total": int, "wins": int, "losses": int,
#       "current_win_streak": int, "current_loss_streak": int,
#       "max_win_streak": int, "max_loss_streak": int
#     }
#   }
# =============================================================================


def empty_state() -> Dict[str, Any]:
    return {
        "indicator": {
            "last_bar_time": None,
            "prev_close": None,
            "ema50": None,
            "ema200": None,
            "atr14": None,
            "bars_processed": 0,
        },
        "trade": {
            "active": False,
            "direction": None,
            "entry": None,
            "sl": None,
            "tp": None,
            "entry_bar_time": None,
        },
        "stats": {
            "total": 0,
            "wins": 0,
            "losses": 0,
            "current_win_streak": 0,
            "current_loss_streak": 0,
            "max_win_streak": 0,
            "max_loss_streak": 0,
        },
    }


STATE: Dict[str, Any] = empty_state()


def load_state() -> None:
    """Load state.json if it exists. Indicator state is informational here —
    we always rebuild it from market data on startup. Trade state matters."""
    global STATE
    if not STATE_FILE.exists():
        STATE = empty_state()
        return
    try:
        with STATE_FILE.open("r") as f:
            disk = json.load(f)
        # Merge into a fresh empty state to tolerate schema additions.
        STATE = empty_state()
        for top in ("indicator", "trade", "stats"):
            if top in disk and isinstance(disk[top], dict):
                STATE[top].update(disk[top])
        log.info("Loaded state.json (trade_active=%s)", STATE["trade"]["active"])
    except Exception as e:
        log.error("Failed to read state.json (%s) — starting fresh", e)
        STATE = empty_state()


def save_state() -> None:
    """Atomic write to state.json."""
    tmp = STATE_FILE.with_suffix(".json.tmp")
    try:
        with tmp.open("w") as f:
            json.dump(STATE, f, indent=2, default=str)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log.error("Failed to save state.json: %s", e)


# =============================================================================
# TWELVE DATA CLIENT
# =============================================================================

BASE_URL = "https://api.twelvedata.com"

async def td_time_series(
    outputsize: Optional[int] = None,
    start_date: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "timezone": "UTC",
        "apikey": TWELVEDATA_API_KEY,
        "order": "asc",
    }
    if start_date is not None:
        params["start_date"] = (start_date + timedelta(seconds=1)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        # outputsize tells TD how far back to look; start_date truncates the bottom.
        # We use GAP_FILL_THRESHOLD+10 so any realistic gap fits in one fetch.
        # No end_date — avoids 400 errors when the latest bar isn't published yet.
        params["outputsize"] = GAP_FILL_THRESHOLD + 10
    elif outputsize is not None:
        params["outputsize"] = outputsize

    last_err: Optional[Exception] = None
    for attempt in range(MAX_FETCH_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.get(f"{BASE_URL}/time_series", params=params)
                if r.status_code >= 400:
                    try:
                        err_body = r.json()
                        err_msg = err_body.get("message", r.text)
                        err_code = err_body.get("code", r.status_code)
                    except Exception:
                        err_msg = r.text
                        err_code = r.status_code
                    log.warning("TD API error %s: %s", err_code, err_msg)
                    r.raise_for_status()
                data = r.json()
            if isinstance(data, dict) and data.get("status") == "error":
                code = data.get("code")
                msg = data.get("message", "")
                if code in (400, 404) and "No data" in msg:
                    return []
                raise RuntimeError(f"Twelve Data error {code}: {msg}")
            values = data.get("values", []) or []
            bars = [
                {
                    "time": parse_td_time(v["datetime"]),
                    "open": float(v["open"]),
                    "high": float(v["high"]),
                    "low": float(v["low"]),
                    "close": float(v["close"]),
                }
                for v in values
            ]
            return bars
        except Exception as e:
            last_err = e
            if attempt < MAX_FETCH_RETRIES:
                log.warning(
                    "Fetch failed (attempt %d/%d): %s — retrying in %ds",
                    attempt + 1,
                    MAX_FETCH_RETRIES + 1,
                    e,
                    RETRY_SLEEP_SEC,
                )
                await asyncio.sleep(RETRY_SLEEP_SEC)
            else:
                log.error("Fetch failed after %d attempts: %s", attempt + 1, e)
    raise last_err if last_err else RuntimeError("Unknown fetch failure")
# =============================================================================
# COLD START — backfill and warm indicators (silent, no alerts)
# =============================================================================


async def cold_start_indicators() -> None:
    """
    Fetch BACKFILL_BARS bars and warm all indicator state.
    Does NOT run the strategy state machine — historical signals are not alerted.
    Trade state is preserved (a trade open at last shutdown is still open).
    """
    log.info("Cold start: fetching %d historical 5-min bars...", BACKFILL_BARS)
    bars = await td_time_series(outputsize=BACKFILL_BARS)
    if len(bars) < EMA_SLOW_LEN + ATR_LEN + 5:
        raise RuntimeError(
            f"Backfill returned only {len(bars)} bars — not enough to warm EMA200/ATR14"
        )

    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    closes = [b["close"] for b in bars]

    ema50_series = seed_ema_series(closes, EMA_FAST_LEN)
    ema200_series = seed_ema_series(closes, EMA_SLOW_LEN)
    atr_series = seed_atr_series(highs, lows, closes, ATR_LEN)

    atr_final = atr_series[-1]
    if atr_final is None:
        raise RuntimeError("ATR could not be computed during backfill")

    STATE["indicator"] = {
        "last_bar_time": bars[-1]["time"].isoformat(),
        "prev_close": closes[-1],
        "ema50": ema50_series[-1],
        "ema200": ema200_series[-1],
        "atr14": atr_final,
        "bars_processed": len(bars),
    }
    save_state()

    log.info(
        "Indicators warmed. Latest bar=%s  close=%.3f  ema50=%.3f  ema200=%.3f  atr14=%.3f",
        fmt_utc(bars[-1]["time"]),
        closes[-1],
        ema50_series[-1],
        ema200_series[-1],
        atr_final,
    )


# =============================================================================
# STATE-MACHINE PROCESSING — runs the EMA Pullback strategy on a bar
# =============================================================================


def evaluate_bar(bar: Dict[str, Any], silent: bool = False) -> List[Dict[str, Any]]:
    """
    Process a single closed 5-min bar through the indicator + strategy state machine.
    Returns a list of pending alerts (entry/outcome) that the caller should send.
    If silent=True, no alerts are emitted (used for historical/gap replay where we
    only want indicators advanced without flooding the inbox — but we use silent=False
    for gap fill since the user wants to catch missed signals).
    """
    ind = STATE["indicator"]
    trade = STATE["trade"]
    stats = STATE["stats"]

    o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
    bar_time: datetime = bar["time"]

    # ---- 1. Advance indicators using this bar ----
    prev_ema50 = ind["ema50"]
    prev_ema200 = ind["ema200"]
    prev_atr = ind["atr14"]
    prev_close_val = ind["prev_close"]

    if None in (prev_ema50, prev_ema200, prev_atr, prev_close_val):
        # Should never happen post cold-start. Fail loudly.
        raise RuntimeError("Indicator state has None values — aborting bar processing")

    ema50_new = step_ema(prev_ema50, c, EMA_FAST_LEN)
    ema200_new = step_ema(prev_ema200, c, EMA_SLOW_LEN)
    atr_new = step_atr(prev_atr, h, l, prev_close_val, ATR_LEN)

    alerts: List[Dict[str, Any]] = []

    # ---- 2. EXIT CHECK (must run before entry check, mirrors Pine ordering) ----
    if trade["active"]:
        direction = trade["direction"]
        entry = trade["entry"]
        sl = trade["sl"]
        tp = trade["tp"]
        is_long = direction == "BUY"

        if is_long:
            tp_hit = h >= tp
            sl_hit = l <= sl
        else:
            tp_hit = l <= tp
            sl_hit = h >= sl

        if tp_hit or sl_hit:
            # Conservative: SL wins on same-bar conflict (matches Pine default)
            if tp_hit and sl_hit:
                outcome = "SL"
            else:
                outcome = "TP" if tp_hit else "SL"
            level = tp if outcome == "TP" else sl

            # Gap exit logic — if bar opened past the level in the resolving
            # direction, the exit is at the open, not the level.
            if is_long:
                if outcome == "TP":
                    exit_px = o if o >= tp else tp
                else:
                    exit_px = o if o <= sl else sl
            else:
                if outcome == "TP":
                    exit_px = o if o <= tp else tp
                else:
                    exit_px = o if o >= sl else sl

            pnl = (exit_px - entry) if is_long else (entry - exit_px)

            # Stats
            stats["total"] += 1
            if outcome == "TP":
                stats["wins"] += 1
                stats["current_win_streak"] += 1
                stats["current_loss_streak"] = 0
                if stats["current_win_streak"] > stats["max_win_streak"]:
                    stats["max_win_streak"] = stats["current_win_streak"]
            else:
                stats["losses"] += 1
                stats["current_loss_streak"] += 1
                stats["current_win_streak"] = 0
                if stats["current_loss_streak"] > stats["max_loss_streak"]:
                    stats["max_loss_streak"] = stats["current_loss_streak"]

            log.info(
                "%s HIT  dir=%s  entry=%.3f  exit=%.3f  pnl=%.3f  bar=%s",
                outcome, direction, entry, exit_px, pnl, fmt_utc(bar_time),
            )

            if not silent:
                alerts.append({
                    "kind": "outcome",
                    "outcome": outcome,
                    "direction": direction,
                    "entry": entry,
                    "sl": sl,
                    "tp": tp,
                    "exit": exit_px,
                    "pnl": pnl,
                    "bar_time": bar_time,
                    "entry_bar_time": _parse_iso(trade["entry_bar_time"]),
                    "is_gap": abs(exit_px - level) > 0.05,
                })

            # Clear trade
            STATE["trade"] = {
                "active": False, "direction": None, "entry": None,
                "sl": None, "tp": None, "entry_bar_time": None,
            }
            trade = STATE["trade"]

    # ---- 3. ENTRY CHECK (only if not in a trade) ----
    if not trade["active"]:
        trend_up = c > ema200_new
        trend_down = c < ema200_new
        long_sig = trend_up and (l <= ema50_new) and (c > ema50_new) and (c > o)
        short_sig = trend_down and (h >= ema50_new) and (c < ema50_new) and (c < o)

        if long_sig or short_sig:
            direction = "BUY" if long_sig else "SELL"
            entry = c  # "Signal close" model
            if direction == "BUY":
                sl = entry - ATR_MULT * atr_new
                tp = entry + RR_RATIO * (entry - sl)
            else:
                sl = entry + ATR_MULT * atr_new
                tp = entry - RR_RATIO * (sl - entry)

            # Sanity check: SL must be distinct from entry by at least 5 cents
            if abs(entry - sl) > 0.05:
                STATE["trade"] = {
                    "active": True,
                    "direction": direction,
                    "entry": entry,
                    "sl": sl,
                    "tp": tp,
                    "entry_bar_time": bar_time.isoformat(),
                }
                log.info(
                    "ENTRY  dir=%s  entry=%.3f  sl=%.3f  tp=%.3f  atr=%.3f  bar=%s",
                    direction, entry, sl, tp, atr_new, fmt_utc(bar_time),
                )
                if not silent and is_alert_time_allowed(bar_time):
                    alerts.append({
                        "kind": "entry",
                        "direction": direction,
                        "entry": entry,
                        "sl": sl,
                        "tp": tp,
                        "atr": atr_new,
                        "bar_time": bar_time,
                    })
                elif not silent:
                    log.info("Entry alert suppressed (outside ALERT_TIME_WINDOWS)")

    # ---- 4. Commit advanced indicator state ----
    ind["ema50"] = ema50_new
    ind["ema200"] = ema200_new
    ind["atr14"] = atr_new
    ind["prev_close"] = c
    ind["last_bar_time"] = bar_time.isoformat()
    ind["bars_processed"] += 1

    return alerts


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


# =============================================================================
# ALERTING (email)
# =============================================================================


def _format_entry_email(a: Dict[str, Any]) -> tuple[str, str]:
    direction = a["direction"]
    entry = a["entry"]
    sl = a["sl"]
    tp = a["tp"]
    atr = a["atr"]
    bar_time: datetime = a["bar_time"]
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    arrow = "🟢" if direction == "BUY" else "🔴"

    subject = f"[{SYMBOL_LABEL}] {direction} signal @ {entry:.2f}"
    body = (
        f"{arrow}  {SYMBOL_LABEL}  {direction}  —  {STRATEGY_LABEL}  (5min)\n"
        f"\n"
        f"Entry : {entry:.2f}\n"
        f"SL    : {sl:.2f}   ({-risk if direction == 'BUY' else risk:+.2f})\n"
        f"TP    : {tp:.2f}   ({reward if direction == 'BUY' else -reward:+.2f})\n"
        f"RR    : 1:{RR_RATIO:.0f}\n"
        f"ATR   : {atr:.2f}\n"
        f"\n"
        f"Bar    : {fmt_utc(bar_time)}\n"
        f"Alerted: {fmt_display(utcnow())}\n"
    )
    return subject, body


def _format_outcome_email(a: Dict[str, Any]) -> tuple[str, str]:
    outcome = a["outcome"]
    direction = a["direction"]
    entry = a["entry"]
    exit_px = a["exit"]
    pnl = a["pnl"]
    bar_time: datetime = a["bar_time"]
    entry_bar_time: Optional[datetime] = a.get("entry_bar_time")
    is_gap = a.get("is_gap", False)
    icon = "✅" if outcome == "TP" else "❌"

    duration_str = ""
    if entry_bar_time:
        delta = bar_time - entry_bar_time
        mins = int(delta.total_seconds() / 60)
        hours = mins // 60
        rem = mins % 60
        duration_str = f"{hours}h {rem}m" if hours else f"{rem}m"

    stats = STATE["stats"]
    win_rate = (stats["wins"] / stats["total"] * 100.0) if stats["total"] else 0.0

    subject = f"[{SYMBOL_LABEL}] {outcome} hit — {direction} closed @ {exit_px:.2f}"
    body = (
        f"{icon}  {SYMBOL_LABEL}  {outcome} HIT  on  {direction}  —  {STRATEGY_LABEL}\n"
        f"\n"
        f"Entry : {entry:.2f}\n"
        f"Exit  : {exit_px:.2f}{'  (gap)' if is_gap else ''}\n"
        f"PnL   : {pnl:+.2f}\n"
        f"Lasted: {duration_str or 'n/a'}\n"
        f"\n"
        f"Stats : {stats['wins']}W / {stats['losses']}L of {stats['total']}   "
        f"(WR {win_rate:.1f}%)\n"
        f"Streak: win {stats['current_win_streak']} / loss {stats['current_loss_streak']}   "
        f"(max W {stats['max_win_streak']} / max L {stats['max_loss_streak']})\n"
        f"\n"
        f"Bar    : {fmt_utc(bar_time)}\n"
        f"Alerted: {fmt_display(utcnow())}\n"
    )
    return subject, body


def _send_email_blocking(subject: str, body: str) -> None:
    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)


async def send_alert(alert: Dict[str, Any]) -> None:
    if alert["kind"] == "entry":
        subject, body = _format_entry_email(alert)
    elif alert["kind"] == "outcome":
        subject, body = _format_outcome_email(alert)
    else:
        log.warning("Unknown alert kind: %r", alert["kind"])
        return

    try:
        await asyncio.to_thread(_send_email_blocking, subject, body)
        log.info("Email sent: %s", subject)
    except Exception as e:
        log.error("Failed to send email (%s): %s", subject, e)


# =============================================================================
# WORKER LOOP
# =============================================================================


async def fetch_and_process_new_bars() -> None:
    """Fetch any bars after last_bar_time and process them in order."""
    ind = STATE["indicator"]
    last_bar_time_str = ind["last_bar_time"]
    last_bar_time = _parse_iso(last_bar_time_str) if last_bar_time_str else None

    if last_bar_time is None:
        log.warning("No last_bar_time — triggering cold start")
        await cold_start_indicators()
        return

    new_bars = await td_time_series(start_date=last_bar_time)

    # Defensive: drop any bar at or before last_bar_time (shouldn't happen with +1s offset)
    new_bars = [b for b in new_bars if b["time"] > last_bar_time]

    if not new_bars:
        log.debug("No new bars (last=%s)", fmt_utc(last_bar_time))
        return

    # Detect huge gap — if so, rebuild indicators from a fresh 600-bar backfill.
    # Note: this DROPS the gap-period state-machine processing; alerts for
    # signals during a multi-day outage are not retroactively sent.
    if len(new_bars) > GAP_FILL_THRESHOLD:
        log.warning(
            "Detected %d bars of gap (> %d) — performing fresh cold start. "
            "Signals during this gap will NOT be alerted.",
            len(new_bars), GAP_FILL_THRESHOLD,
        )
        await cold_start_indicators()
        return

    log.info("Processing %d new/missed bar(s) since %s",
             len(new_bars), fmt_utc(last_bar_time))

    pending_alerts: List[Dict[str, Any]] = []
    for bar in new_bars:
        pending_alerts.extend(evaluate_bar(bar, silent=False))

    save_state()

    # Send alerts after committing state, so a send failure doesn't leave us
    # in an inconsistent place.
    for alert in pending_alerts:
        await send_alert(alert)


async def wait_until(target: datetime) -> None:
    """Sleep until UTC `target`. Returns immediately if already past."""
    while True:
        now = utcnow()
        delta = (target - now).total_seconds()
        if delta <= 0:
            return
        # Cap individual sleeps so cancellation is responsive
        await asyncio.sleep(min(delta, 60.0))


async def worker_loop() -> None:
    """Main loop. Survives any per-iteration exception."""
    log.info("Worker loop started")

    # --- Boot: rebuild indicators from market data ---
    while True:
        try:
            await cold_start_indicators()
            break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("Cold start failed: %s — retrying in %ds", e, LOOP_EXCEPTION_SLEEP_SEC)
            await asyncio.sleep(LOOP_EXCEPTION_SLEEP_SEC)

    # --- Steady-state loop ---
    while True:
        try:
            now = utcnow()

            if is_market_closed(now):
                open_at = next_market_open_utc(now)
                log.info("Market closed. Sleeping until %s", fmt_utc(open_at))
                await wait_until(open_at)
                # After waking, refresh indicators to absorb the weekend gap cleanly.
                log.info("Market reopening — refreshing indicators from market")
                try:
                    await cold_start_indicators()
                except Exception as e:
                    log.error("Post-weekend refresh failed: %s", e)
                continue

            next_close = next_bar_close_utc(now)
            fetch_at = next_close + timedelta(seconds=POST_CLOSE_BUFFER_SEC)
            log.debug("Next fetch at %s", fmt_utc(fetch_at))
            await wait_until(fetch_at)

            await fetch_and_process_new_bars()

        except asyncio.CancelledError:
            log.info("Worker loop cancelled — exiting")
            raise
        except Exception as e:
            log.exception("Loop iteration failed: %s — sleeping %ds and continuing",
                          e, LOOP_EXCEPTION_SLEEP_SEC)
            await asyncio.sleep(LOOP_EXCEPTION_SLEEP_SEC)


# =============================================================================
# FASTAPI APP
# =============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state()
    import time as _time
    system_tz = _time.tzname
    log.info("Service starting (symbol=%s, interval=%s, display_tz=%s, system_tz=%s)",
             SYMBOL, INTERVAL, DISPLAY_TIMEZONE, system_tz)
    task = asyncio.create_task(worker_loop())
    try:
        yield
    finally:
        log.info("Service shutting down")
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="XAUUSD Signal Alerter", lifespan=lifespan)


@app.get("/")
async def root():
    return {
        "service": "xau-alerter",
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "strategy": STRATEGY_LABEL,
        "now_utc": utcnow().isoformat(),
    }


@app.get("/state")
async def get_state():
    """Live view of internal state. Useful for debugging on the VPS."""
    return deepcopy(STATE)


@app.get("/health")
async def health():
    ind = STATE["indicator"]
    trade = STATE["trade"]
    return {
        "ok": ind["last_bar_time"] is not None,
        "last_bar_utc": ind["last_bar_time"],
        "bars_processed": ind["bars_processed"],
        "in_trade": trade["active"],
        "direction": trade["direction"],
        "market_closed": is_market_closed(utcnow()),
    }


@app.post("/test-email")
async def test_email():
    """Send a test email so you can confirm SMTP creds are working."""
    try:
        await asyncio.to_thread(
            _send_email_blocking,
            f"[{SYMBOL_LABEL}] Test email from xau-alerter",
            "If you're reading this, SMTP is configured correctly.\n",
        )
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}