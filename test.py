#!/usr/bin/env python3
"""
XAUUSD EMA Pullback Strategy — Backtester + Cross-Verification Tool
=====================================================================

Modes:
  seed                              Populate SQLite DB with 5min candles (2025-01-01 → now)
  test   --days N  [--tf M]         Print every setup in past N days for TV cross-check
  backtest --months N --tf M        Hourly accuracy breakdown for past N months
  live [--once]                     Catch up DB, then poll new 5m candles and log live setups
  clean [--reset-live-signals]        Remove obvious bad weekend/invalid data already in DB

All timestamps are stored and displayed in IST (Asia/Kolkata).

Strategy implemented: Strategy 1 — EMA Pullback (Trend)
  • EMA50 / EMA200 / ATR14  (Pine-faithful math)
  • Entry = signal candle close
  • SL = entry ∓ 1.5×ATR    TP = entry ± 2×SL-distance  (1:2 RR)
  • One trade at a time, conservative SL-first on same-bar conflict
  • Signals calculated on selected TF; TP/SL resolved on raw 5m candles
  • No Modification-1 time windows (raw strategy)
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import sys
import time as _time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
import smtplib
from email.message import EmailMessage
# =====================================================================
# CONFIGURATION — edit these
# =====================================================================

API_KEY = "aca6dd9e986d4fc69bfd46a8ecc7b8d7"                    # Twelve Data API key
DB_FILE = "data.db"                       # SQLite database file
SYMBOL  = "XAU/USD"                              # Twelve Data symbol

# Timezone constants
IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")

# Strategy 1 defaults — must match Pine Script indicator settings
EMA_FAST_LEN = 50
EMA_SLOW_LEN = 200
ATR_LEN      = 14
ATR_MULT     = 1.5
RR_RATIO     = 2.2        # fixed 1:2

# Seeding
SEED_START_UTC     = "2026-01-01 00:00:00"
REQUEST_DELAY_SEC  = 8     # stay under 8 calls/min free-tier limit
MAX_RETRIES        = 3

# Minimum SL distance (in price) to accept a trade
SYMBOL_MINTICK = 0.01  # TradingView syminfo.mintick equivalent for most XAUUSD feeds

# Live polling
# Candle availability model: after a 5m candle closes, wait 90 seconds before
# requesting it. If API still does not have it, retry once after 20 seconds.
LIVE_CANDLE_DELAY_SEC = 90
LIVE_RETRY_DELAY_SEC = 20

# Safety buffer so the live loop never wakes a few milliseconds/seconds before
# the exact API-eligible boundary. This does NOT replace the 90s delay.
# It only prevents missing the boundary and waiting one full extra 5m cycle.
LIVE_SCHEDULER_SAFETY_SEC = 8

LIVE_PENDING_MAX_HOURS = 6
LIVE_LOOKBACK_DAYS = 10
# After a locked trade closes, allow a new setup to alert even if it is
# no longer the absolute latest signal candle, as long as it is recent.
# This fixes cases where TP/SL and the next setup become visible in the
# same live fetch/catch-up cycle.
LIVE_POST_UNLOCK_ALERT_GRACE_BARS = 2

# Market/session sanity filter
# ----------------------------
# Twelve Data can sometimes return stale/flat weekend bars for XAU/USD.
# TradingView/OANDA will not normally produce real gold signals on those bars,
# and they can collapse ATR into nonsense micro-TP/SL values.
#
# The filter is intentionally conservative:
#   • keep early Saturday IST, because Friday's NY session can extend past
#     midnight in India;
#   • reject obvious Saturday daytime/evening, all Sunday, and early Monday
#     before the typical Sunday reopen appears in IST.
# If your broker/session differs, edit these two times or set the flag False.
MARKET_FILTER_ENABLED = True
MARKET_SATURDAY_CUTOFF_IST = "03:30"  # reject Saturday candles at/after this IST time
MARKET_MONDAY_OPEN_IST     = "03:30"  # reject Monday candles before this IST time


# Maintainable rule list for live setup monitoring.
# Window `end` is EXCLUSIVE. For 18:00-20:00, entries at 18:00..19:55 are allowed,
# while an entry at exactly 20:00 is excluded because your 20:00 hour tested weaker.
LIVE_RULES = [
    {
        "name": "S1_EMA_PULLBACK_5M_14_15_IST",
        "enabled": True,
        "tf_min": 5,
        "windows": [
            {"start": "14:00", "end": "15:00"},
        ],
        "strategy": "ema_pullback",
    },
    {
        "name": "S1_EMA_PULLBACK_5M_18_20_IST",
        "enabled": True,
        "tf_min": 5,
        "windows": [
            {"start": "18:00", "end": "20:00"},
        ],
        "strategy": "ema_pullback",
    },
    {
        "name": "S1_EMA_PULLBACK_15M_16_17_IST",
        "enabled": True,
        "tf_min": 15,
        "windows": [
            {"start": "16:00", "end": "17:00"},
        ],
        "strategy": "ema_pullback",
    },
    {
        "name": "S1_EMA_PULLBACK_60M_18_20_IST",
        "enabled": True,
        "tf_min": 60,
        "windows": [
            {"start": "18:00", "end": "20:00"},
        ],
        "strategy": "ema_pullback",
    },
    {
        "name": "S1_EMA_PULLBACK_240M_09_10_IST",
        "enabled": True,
        "tf_min": 240,
        "windows": [
            {"start": "09:00", "end": "10:00"},
        ],
        "strategy": "ema_pullback",
    },
]


# =====================================================================
# DATABASE  (SQLite — single file, zero dependencies)
# =====================================================================

def init_db(db_path: str = DB_FILE) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candles_5m (
            dt   TEXT PRIMARY KEY,   -- IST  'YYYY-MM-DD HH:MM:SS'
            o    REAL NOT NULL,
            h    REAL NOT NULL,
            l    REAL NOT NULL,
            c    REAL NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS live_signals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_name   TEXT NOT NULL,
            tf_min      INTEGER NOT NULL,
            entry_at    TEXT NOT NULL,
            direction   TEXT NOT NULL,
            entry       REAL NOT NULL,
            sl          REAL NOT NULL,
            tp          REAL NOT NULL,
            outcome     TEXT,
            created_at  TEXT NOT NULL,
            UNIQUE(rule_name, tf_min, entry_at, direction, entry)
        )
    """)
    existing_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(live_signals)").fetchall()
    }

    live_signal_new_cols = {
        "exit_bar": "TEXT",
        "exit_px": "REAL",
        "pnl": "REAL",
        "closed_at": "TEXT",
        "setup_notified_at": "TEXT",
        "exit_notified_at": "TEXT",
        "is_silent_lock": "INTEGER DEFAULT 0",
    }

    for col, col_type in live_signal_new_cols.items():
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE live_signals ADD COLUMN {col} {col_type}")
    conn.commit()
    return conn


def upsert_candles(conn: sqlite3.Connection, rows: List[Tuple]) -> int:
    """Insert or ignore candle rows. Returns count inserted."""
    cur = conn.executemany(
        "INSERT OR IGNORE INTO candles_5m (dt, o, h, l, c) VALUES (?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return cur.rowcount


def count_candles(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM candles_5m").fetchone()[0]


def get_range(conn: sqlite3.Connection) -> Tuple[Optional[str], Optional[str]]:
    row = conn.execute(
        "SELECT MIN(dt), MAX(dt) FROM candles_5m"
    ).fetchone()
    return (row[0], row[1]) if row else (None, None)


def load_candles(
    conn: sqlite3.Connection,
    start_ist: Optional[str] = None,
    end_ist: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Load 5-min candles in chronological order."""
    q = "SELECT dt, o, h, l, c FROM candles_5m"
    params: List[str] = []
    clauses: List[str] = []
    if start_ist:
        clauses.append("dt >= ?"); params.append(start_ist)
    if end_ist:
        clauses.append("dt <= ?"); params.append(end_ist)
    if clauses:
        q += " WHERE " + " AND ".join(clauses)
    q += " ORDER BY dt ASC"
    return [
        {"dt": r[0], "o": r[1], "h": r[2], "l": r[3], "c": r[4]}
        for r in conn.execute(q, params).fetchall()
    ]


def candle_exists(conn: sqlite3.Connection, dt_ist: str) -> bool:
    row = conn.execute("SELECT 1 FROM candles_5m WHERE dt = ? LIMIT 1", (dt_ist,)).fetchone()
    return row is not None


def get_latest_candle_dt(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute("SELECT MAX(dt) FROM candles_5m").fetchone()
    return row[0] if row and row[0] else None


def record_live_signal(
    conn: sqlite3.Connection,
    rule_name: str,
    tf_min: int,
    trade: Dict[str, Any],
    *,
    silent_lock: bool = False,
) -> bool:
    """
    Persist a live trade once.

    silent_lock=True means:
      • trade still locks this timeframe
      • no setup alert should be sent
      • used for trades formed outside rule windows
    """
    now_str = _ist_str(_now_ist())

    try:
        conn.execute(
            """
            INSERT INTO live_signals
                (
                    rule_name, tf_min, entry_at, direction, entry, sl, tp,
                    outcome, created_at, setup_notified_at, is_silent_lock
                )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rule_name,
                tf_min,
                trade["entry_at"],
                trade["direction"],
                float(trade["entry"]),
                float(trade["sl"]),
                float(trade["tp"]),
                "OPEN",
                now_str,
                None if silent_lock else now_str,
                1 if silent_lock else 0,
            ),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def get_open_live_trade_for_tf(conn: sqlite3.Connection, tf_min: int) -> Optional[Dict[str, Any]]:
    """
    One active trade per timeframe.
    A 5m open trade blocks only 5m.
    A 15m open trade blocks only 15m.
    """
    row = conn.execute(
        """
        SELECT
            id, rule_name, tf_min, entry_at, direction, entry, sl, tp,
            created_at, COALESCE(is_silent_lock, 0)
        FROM live_signals
        WHERE tf_min = ?
          AND COALESCE(outcome, 'OPEN') = 'OPEN'
        ORDER BY entry_at ASC
        LIMIT 1
        """,
        (tf_min,),
    ).fetchone()

    if not row:
        return None

    return {
        "id": row[0],
        "rule_name": row[1],
        "tf_min": int(row[2]),
        "entry_at": row[3],
        "direction": row[4],
        "entry": float(row[5]),
        "sl": float(row[6]),
        "tp": float(row[7]),
        "created_at": row[8],
        "is_silent_lock": int(row[9]),
    }


def resolve_open_live_trade_on_5m(
    open_trade: Dict[str, Any],
    exec_candles_5m: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    Check whether an already-open live trade has hit TP or SL.
    """
    entry_dt = _parse_ist(open_trade["entry_at"])
    start_idx = _find_exec_start_index(exec_candles_5m, entry_dt)

    return _resolve_trade_on_5m(
        exec_candles_5m=exec_candles_5m,
        start_idx=start_idx,
        direction=open_trade["direction"],
        entry=float(open_trade["entry"]),
        sl=float(open_trade["sl"]),
        tp=float(open_trade["tp"]),
    )


def close_live_signal(
    conn: sqlite3.Connection,
    signal_id: int,
    resolved: Dict[str, Any],
) -> None:
    exit_bar = resolved["exit_bar"]
    closed_at = _ist_str(_parse_ist(exit_bar) + timedelta(minutes=5))

    conn.execute(
        """
        UPDATE live_signals
        SET outcome = ?,
            exit_bar = ?,
            exit_px = ?,
            pnl = ?,
            closed_at = ?
        WHERE id = ?
        """,
        (
            resolved["outcome"],
            resolved["exit_bar"],
            float(resolved["exit_px"]),
            float(resolved["pnl"]),
            closed_at,
            signal_id,
        ),
    )
    conn.commit()


def mark_live_exit_notified(conn: sqlite3.Connection, signal_id: int) -> None:
    conn.execute(
        """
        UPDATE live_signals
        SET exit_notified_at = ?
        WHERE id = ?
        """,
        (_ist_str(_now_ist()), signal_id),
    )
    conn.commit()


def _trade_matches_any_rule_window(
    trade: Dict[str, Any],
    rules: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    Return the first rule whose window allows this trade entry.
    """
    for rule in rules:
        if _entry_in_windows(trade["entry_at"], rule["windows"]):
            return rule
    return None

def _is_post_unlock_setup_alertable(
    trade: Dict[str, Any],
    just_closed_exit_bar: Optional[str],
    latest_signal_close_str: str,
    tf_min: int,
) -> bool:
    """
    Return True when a setup is allowed to alert even though it is not the
    latest signal close, because the previous timeframe lock closed in this
    same evaluation cycle.

    This fixes:
      • previous trade TP/SL at 14:20
      • new setup entry_at 14:20 or shortly after
      • latest signal close already advanced to 14:25/14:30 during catch-up
    """
    if just_closed_exit_bar is None:
        return False

    entry_dt = _parse_ist(trade["entry_at"])
    exit_dt = _parse_ist(just_closed_exit_bar)
    latest_dt = _parse_ist(latest_signal_close_str)

    # The new setup must be at/after the old trade's exit-bar timestamp.
    # This matches run_strategy(), which allows a new signal when
    # sig_close_dt == locked_until_exit_dt.
    if entry_dt < exit_dt:
        return False

    # Do not alert very old setups after a long cold start.
    oldest_allowed = latest_dt - timedelta(minutes=tf_min * LIVE_POST_UNLOCK_ALERT_GRACE_BARS)
    return entry_dt >= oldest_allowed

# =====================================================================
# TWELVE DATA API  (stdlib only — no requests/httpx needed)
# =====================================================================

def _api_get(params: Dict[str, str]) -> dict:
    base = "https://api.twelvedata.com/time_series"
    qs = "&".join(f"{k}={urllib.request.quote(str(v), safe='/:')}"
                  for k, v in params.items())
    url = f"{base}?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": "xau-backtest/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def fetch_page(
    start_utc: str,
    end_utc: str,
) -> List[Dict[str, Any]]:
    """
    Fetch 5-min bars from Twelve Data (oldest-first).

    IMPORTANT: uses start_date + end_date WITHOUT outputsize.
    Per TD docs, outputsize restricts output when used with date params,
    and when start_date is far in the past with outputsize, the API
    returns the most recent N bars instead of starting from start_date.
    """
    params = {
        "symbol":     SYMBOL,
        "interval":   "5min",
        "timezone":   "UTC",
        "apikey":     API_KEY,
        "order":      "asc",
        "dp":         "5",
        "start_date": start_utc,
        "end_date":   end_utc,
    }
    data = _api_get(params)
    if isinstance(data, dict) and data.get("status") == "error":
        msg = data.get("message", "")
        if "No data" in msg or "no data" in msg.lower():
            return []
        raise RuntimeError(f"API error {data.get('code')}: {msg}")
    values = data.get("values") or []
    return [
        {
            "dt_utc": v["datetime"],
            "o": float(v["open"]),
            "h": float(v["high"]),
            "l": float(v["low"]),
            "c": float(v["close"]),
        }
        for v in values
    ]


def utc_str_to_ist_str(utc_str: str) -> str:
    """'2025-01-02 00:05:00' UTC → '2025-01-02 05:35:00' IST."""
    dt = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")


def ist_str_to_utc_str(ist_str: str) -> str:
    dt = datetime.strptime(ist_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _floor_to_5m(dt: datetime) -> datetime:
    """Floor an aware datetime to its 5-minute boundary."""
    dt = dt.astimezone(IST)
    floored_minute = (dt.minute // 5) * 5
    return dt.replace(minute=floored_minute, second=0, microsecond=0)


def latest_api_eligible_5m_open(now_ist: Optional[datetime] = None) -> Optional[datetime]:
    """
    Latest 5m candle open time that should be safe to request from the API.

    A candle open at 18:00 closes at 18:05. With a 90-second delay, it becomes
    eligible at 18:06:30. Before that, we do not request it in live mode.
    """
    now = now_ist or _now_ist()
    safe_clock = now - timedelta(seconds=LIVE_CANDLE_DELAY_SEC)
    latest_close_boundary = _floor_to_5m(safe_clock)
    latest_open = latest_close_boundary - timedelta(minutes=5)
    return latest_open



def _hhmm_config_to_minutes(value: str) -> int:
    """Convert 'HH:MM' config values to minutes since midnight."""
    hh, mm = value.split(":", 1)
    return int(hh) * 60 + int(mm)


def is_valid_xau_market_time_ist(dt_ist: str) -> bool:
    """
    Return False for obvious XAU/USD closed-market weekend candles.

    This protects all modes from stale API bars that are not present on the
    TradingView/OANDA chart and that can make ATR collapse to micro values.
    """
    if not MARKET_FILTER_ENABLED:
        return True

    dt = datetime.strptime(dt_ist, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
    weekday = dt.weekday()  # Mon=0 ... Sun=6
    minute_of_day = dt.hour * 60 + dt.minute
    sat_cutoff = _hhmm_config_to_minutes(MARKET_SATURDAY_CUTOFF_IST)
    mon_open = _hhmm_config_to_minutes(MARKET_MONDAY_OPEN_IST)

    # Friday's NY close can be early Saturday IST, so only reject obvious
    # Saturday daytime/evening bars.
    if weekday == 5 and minute_of_day >= sat_cutoff:
        return False
    if weekday == 6:
        return False
    if weekday == 0 and minute_of_day < mon_open:
        return False
    return True


def is_valid_ohlc_values(o: float, h: float, l: float, c: float) -> bool:
    """Basic OHLC sanity check before data is allowed into calculations."""
    vals = (o, h, l, c)
    if not all(isinstance(v, (int, float)) and math.isfinite(float(v)) for v in vals):
        return False
    return h >= max(o, c) and l <= min(o, c) and h >= l


def is_valid_5m_candle_row(c: Dict[str, Any]) -> bool:
    """Combined market-time + OHLC validation for an existing DB candle row."""
    return (
        is_valid_xau_market_time_ist(c["dt"])
        and is_valid_ohlc_values(float(c["o"]), float(c["h"]), float(c["l"]), float(c["c"]))
    )


def fetch_and_store_5m_range(
    conn: sqlite3.Connection,
    start_ist_dt: datetime,
    end_ist_dt: datetime,
    *,
    verbose: bool = True,
) -> Tuple[int, int]:
    """
    Fetch and store closed 5m candles for an IST open-time range, inclusive.

    Returns: (api_bars_seen, newly_inserted_rows)
    """
    if end_ist_dt < start_ist_dt:
        return (0, 0)

    # Keep API windows safely below provider limits.
    window_start = start_ist_dt
    total_bars = 0
    total_inserted = 0
    window_days = 14

    while window_start <= end_ist_dt:
        window_end = min(window_start + timedelta(days=window_days), end_ist_dt)
        ws_utc = window_start.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
        we_utc = window_end.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")

        if verbose:
            print(f"  Fetch 5m: {_ist_str(window_start)} → {_ist_str(window_end)} IST … ", end="", flush=True)

        bars = fetch_page(ws_utc, we_utc)
        rows = []
        rejected = 0
        for b in bars:
            dt_ist = utc_str_to_ist_str(b["dt_utc"])
            # Guard against providers returning a slightly wider range.
            if not (_ist_str(start_ist_dt) <= dt_ist <= _ist_str(end_ist_dt)):
                continue
            if not is_valid_xau_market_time_ist(dt_ist):
                rejected += 1
                continue
            if not is_valid_ohlc_values(b["o"], b["h"], b["l"], b["c"]):
                rejected += 1
                continue
            rows.append((dt_ist, b["o"], b["h"], b["l"], b["c"]))

        inserted = upsert_candles(conn, rows) if rows else 0
        total_bars += len(rows)
        total_inserted += inserted

        if verbose:
            reject_note = f", {rejected} rejected" if rejected else ""
            print(f"{len(rows)} bars ({inserted} new{reject_note})")

        window_start = window_end + timedelta(minutes=5)
        if window_start <= end_ist_dt:
            _time.sleep(REQUEST_DELAY_SEC)

    return total_bars, total_inserted


def catch_up_db_to_latest_eligible(conn: sqlite3.Connection) -> None:
    """
    On live startup, fill the DB gap between the latest seeded candle and the
    latest API-safe closed candle before monitoring begins.
    """
    latest_eligible = latest_api_eligible_5m_open()
    if latest_eligible is None:
        return

    latest = get_latest_candle_dt(conn)
    if latest:
        start = _parse_ist(latest) + timedelta(minutes=5)
    else:
        start = datetime.strptime(SEED_START_UTC, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC).astimezone(IST)

    if start > latest_eligible:
        print(f"  DB already up to date through {_ist_str(latest_eligible)} IST")
        return

    print(f"  Catch-up needed: {_ist_str(start)} → {_ist_str(latest_eligible)} IST")
    try:
        fetch_and_store_5m_range(conn, start, latest_eligible, verbose=True)
    except Exception as e:
        print(f"  ⚠ Catch-up fetch failed: {e}")


def fetch_live_tick_candles(conn: sqlite3.Connection, pending_opens: set[str]) -> set[str]:
    """
    Fetch the latest eligible 5m candle plus unresolved recent pending candles.

    If the API does not return the latest candle, retry once after 20 seconds.
    If still unavailable, keep it pending. On the next 5m cycle, this function
    requests both the old pending candle(s) and the new current candle.
    """
    now = _now_ist()
    latest_open = latest_api_eligible_5m_open(now)
    if latest_open is None:
        return pending_opens

    print(
        f"  Live eligibility check: now {_ist_str(now)} IST | "
        f"latest eligible 5m open {_ist_str(latest_open)} IST | "
        f"expected close {_ist_str(latest_open + timedelta(minutes=5))} IST"
    )

    latest_open_str = _ist_str(latest_open)
    cutoff = now - timedelta(hours=LIVE_PENDING_MAX_HOURS)
    pending_opens = {p for p in pending_opens if _parse_ist(p) >= cutoff}
    targets = set(pending_opens)
    targets.add(latest_open_str)

    # Also avoid holes if the DB is simply behind by a few candles.
    latest_db = get_latest_candle_dt(conn)
    if latest_db:
        db_next = _parse_ist(latest_db) + timedelta(minutes=5)
        start_dt = min([_parse_ist(t) for t in targets] + [db_next])
    else:
        start_dt = min(_parse_ist(t) for t in targets)
    end_dt = latest_open

    if start_dt > end_dt:
        return pending_opens

    def _attempt(label: str) -> None:
        print(f"\n  Live candle fetch {label}: {_ist_str(start_dt)} → {_ist_str(end_dt)} IST")
        fetch_and_store_5m_range(conn, start_dt, end_dt, verbose=True)

    try:
        _attempt("attempt 1")
    except Exception as e:
        print(f"  ⚠ Live fetch attempt 1 failed: {e}")

    missing = {t for t in targets if not candle_exists(conn, t)}
    if latest_open_str in missing:
        print(f"  Latest candle {latest_open_str} not available yet. Retrying once in {LIVE_RETRY_DELAY_SEC}s …")
        _time.sleep(LIVE_RETRY_DELAY_SEC)
        try:
            _attempt("retry")
        except Exception as e:
            print(f"  ⚠ Live fetch retry failed: {e}")

    unresolved = {t for t in targets if not candle_exists(conn, t)}
    if unresolved:
        print("  Pending candle(s) for next cycle: " + ", ".join(sorted(unresolved)))
    return unresolved


# =====================================================================
# SEED COMMAND
# =====================================================================

def cmd_seed() -> None:
    """
    Paginate through history in ~14-day windows using start_date + end_date
    (no outputsize). Each window yields ≤ ~3800 bars for 5min gold, safely
    under the 5000-per-request API cap.
    """
    WINDOW_DAYS = 14        # calendar days per request window

    conn = init_db()
    _, latest = get_range(conn)

    if latest:
        last_ist = datetime.strptime(latest, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
        window_start = last_ist.astimezone(UTC) + timedelta(minutes=1)
        print(f"  DB has data up to {latest} IST — resuming from there.\n")
    else:
        window_start = datetime.strptime(SEED_START_UTC, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=UTC
        )
        print(f"  Fresh seed from {SEED_START_UTC} UTC.\n")

    now_utc = datetime.now(UTC)
    page = 0
    total_new = 0
    empty_streak = 0          # consecutive windows with 0 bars
    got_data_ever = False      # have we received any bars at all?

    while window_start < now_utc:
        page += 1
        window_end = min(window_start + timedelta(days=WINDOW_DAYS), now_utc)
        ws = window_start.strftime("%Y-%m-%d %H:%M:%S")
        we = window_end.strftime("%Y-%m-%d %H:%M:%S")

        print(f"  Page {page:>3}  |  {ws}  →  {we} UTC … ", end="", flush=True)

        bars = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                bars = fetch_page(ws, we)
                break
            except Exception as e:
                wait = 10 * attempt
                print(f"\n           ⚠  attempt {attempt} failed ({e}), retry in {wait}s … ",
                      end="", flush=True)
                _time.sleep(wait)

        if bars is None:
            print("FAILED after retries — stopping.")
            break

        if not bars:
            empty_streak += 1
            if got_data_ever:
                # We had data before but now getting empties → past available range
                print(f"0 bars (gap after data)")
                if empty_streak > 3:
                    print("  ✓  3+ empty windows after data — seed complete.")
                    break
            else:
                # Haven't found data yet — API may not have history this far back
                print(f"0 bars (no data yet — skipping)")
            window_start = window_end + timedelta(seconds=1)
            # Shorter pause for empty windows (still counts as an API call)
            _time.sleep(2)
            continue

        empty_streak = 0
        got_data_ever = True

        # Convert UTC → IST, insert
        rows = [(utc_str_to_ist_str(b["dt_utc"]), b["o"], b["h"], b["l"], b["c"])
                for b in bars]
        inserted = upsert_candles(conn, rows)
        total_new += inserted

        first_ist = rows[0][0]
        last_ist_str = rows[-1][0]
        print(f"{len(bars):>5} bars  →  {first_ist}  …  {last_ist_str} IST  "
              f"({inserted} new)")

        # Advance window
        window_start = window_end + timedelta(seconds=1)

        print(f"           ⏳  rate-limit pause ({REQUEST_DELAY_SEC}s)")
        _time.sleep(REQUEST_DELAY_SEC)

    mn, mx = get_range(conn)
    total = count_candles(conn)
    conn.close()

    print(f"\n{'═'*64}")
    print(f"  Seed complete.")
    print(f"  DB file   : {DB_FILE}")
    print(f"  Candles   : {total:,}")
    if mn and mx:
        print(f"  Range     : {mn}  →  {mx}  IST")
    print(f"  New rows  : {total_new:,}")
    print(f"{'═'*64}\n")


# =====================================================================
# CANDLE AGGREGATION  (5 min → any higher TF)
# =====================================================================

def aggregate_to_tf(candles_5m: List[Dict], tf_min: int) -> List[Dict]:
    """
    Group 5-min candles into `tf_min`-minute candles.

    Alignment is epoch-based (matching TradingView forex candle boundaries):
      • 5, 15, 30 → clean IST boundaries (IST offset 330 min divides evenly)
      • 60         → starts at :30 in IST (because 330 mod 60 = 30) — same as TV
      • 240        → epoch-aligned 4 H blocks — same as TV

    The candle 'dt' is set to the boundary start time (= candle open time).
    """
    if tf_min == 5:
        return candles_5m

    if 60 % tf_min != 0 and tf_min not in (240,):
        print(f"  ⚠  TF {tf_min}min may not align cleanly. Recommended: 5,15,30,60,240.")

    buckets: Dict[int, Dict] = {}          # boundary_epoch_min → OHLC
    order: List[int] = []                   # insertion order

    for c in candles_5m:
        dt = datetime.strptime(c["dt"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
        epoch_min = int(dt.timestamp()) // 60
        boundary  = (epoch_min // tf_min) * tf_min

        if boundary not in buckets:
            bdt = datetime.fromtimestamp(boundary * 60, tz=IST)
            buckets[boundary] = {
                "dt": bdt.strftime("%Y-%m-%d %H:%M:%S"),
                "o":  c["o"],
                "h":  c["h"],
                "l":  c["l"],
                "c":  c["c"],
            }
            order.append(boundary)
        else:
            b = buckets[boundary]
            b["h"] = max(b["h"], c["h"])
            b["l"] = min(b["l"], c["l"])
            b["c"] = c["c"]          # last close wins

    return [buckets[k] for k in order]


# =====================================================================
# INDICATORS  (Pine-Script-faithful math)
# =====================================================================

def compute_ema(closes: List[float], length: int) -> List[float]:
    """
    Pine ta.ema():  alpha = 2/(len+1)
      ema[0] = close[0]
      ema[i] = alpha*close[i] + (1-alpha)*ema[i-1]
    """
    alpha = 2.0 / (length + 1.0)
    out = [closes[0]]
    for i in range(1, len(closes)):
        out.append(alpha * closes[i] + (1.0 - alpha) * out[-1])
    return out


def compute_atr(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    length: int,
) -> List[Optional[float]]:
    """
    Pine ta.atr() = ta.rma(TR, length).
      TR[0] = high-low
      TR[i] = max(high-low, |high-prevClose|, |low-prevClose|)
      RMA seed (at index length-1) = SMA(TR[0..length-1])
      RMA[i]  = (RMA[i-1]*(length-1) + TR[i]) / length
    Returns None for first (length-1) bars.
    """
    n = len(closes)
    trs: List[float] = []
    for i in range(n):
        if i == 0:
            trs.append(highs[i] - lows[i])
        else:
            trs.append(max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i]  - closes[i - 1]),
            ))

    atrs: List[Optional[float]] = [None] * n
    if n < length:
        return atrs
    seed = sum(trs[:length]) / length
    atrs[length - 1] = seed
    for i in range(length, n):
        atrs[i] = (atrs[i - 1] * (length - 1) + trs[i]) / length   # type: ignore
    return atrs


# =====================================================================
# STRATEGY 1 — EMA PULLBACK
# =====================================================================

def _parse_ist(dt_str: str) -> datetime:
    """Parse 'YYYY-MM-DD HH:MM:SS' as an IST-aware datetime."""
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)


def _ist_str(dt: datetime) -> str:
    """Format an IST-aware datetime as DB string."""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _candle_close_dt(candle_dt_str: str, tf_min: int) -> datetime:
    """Return candle CLOSE time (open + timeframe) as IST datetime."""
    return _parse_ist(candle_dt_str) + timedelta(minutes=tf_min)


def drop_unclosed_5m_candles(candles_5m: List[Dict]) -> List[Dict]:
    """
    Remove the latest still-forming 5m candle, if present.

    TradingView with request.security(..., lookahead_off) only confirms signals on
    closed candles. This avoids Python test mode seeing a partial API candle that
    TradingView has not confirmed yet.
    """
    now = _now_ist()
    out: List[Dict] = []
    dropped = 0
    for c in candles_5m:
        close_dt = _parse_ist(c["dt"]) + timedelta(minutes=5)
        if close_dt <= now:
            out.append(c)
        else:
            dropped += 1
    if dropped:
        print(f"  Dropped {dropped} unclosed 5-min candle(s)")
    return out



def filter_valid_market_candles(candles_5m: List[Dict], *, verbose: bool = True) -> List[Dict]:
    """
    Remove obvious weekend/invalid candles from already-loaded DB data.

    This makes test/backtest/live safe even before you run the clean command.
    """
    out = [c for c in candles_5m if is_valid_5m_candle_row(c)]
    dropped = len(candles_5m) - len(out)
    if verbose and dropped:
        print(f"  Dropped {dropped} invalid/weekend 5-min candle(s)")
    return out


def _latest_5m_close_dt(candles_5m: List[Dict]) -> Optional[datetime]:
    if not candles_5m:
        return None
    return _parse_ist(candles_5m[-1]["dt"]) + timedelta(minutes=5)


def drop_unclosed_signal_candles(
    signal_candles: List[Dict],
    tf_min: int,
    latest_exec_close: Optional[datetime],
) -> List[Dict]:
    """
    Remove incomplete aggregated signal-timeframe candles.

    Example: with TF=15, if only 18:00 and 18:05 raw 5m candles exist,
    the 18:00-18:15 signal candle is still incomplete and must not be used.
    """
    if latest_exec_close is None:
        return []
    out: List[Dict] = []
    dropped = 0
    for c in signal_candles:
        if _candle_close_dt(c["dt"], tf_min) <= latest_exec_close:
            out.append(c)
        else:
            dropped += 1
    if dropped:
        print(f"  Dropped {dropped} unclosed {tf_min}-min signal candle(s)")
    return out


def prepare_strategy_candles(
    candles_5m: List[Dict],
    tf_min: int,
    *,
    verbose: bool = True,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Shared data-prep path for test, backtest, and live.

    All modes now use the same safeguards:
      1. remove obvious weekend/invalid provider bars;
      2. remove unclosed raw 5m bars;
      3. aggregate to signal TF;
      4. remove unclosed aggregated TF bars.
    """
    candles_5m = filter_valid_market_candles(candles_5m, verbose=verbose)
    candles_5m = drop_unclosed_5m_candles(candles_5m)
    latest_exec_close = _latest_5m_close_dt(candles_5m)
    signal_candles = aggregate_to_tf(candles_5m, tf_min)
    signal_candles = drop_unclosed_signal_candles(signal_candles, tf_min, latest_exec_close)
    return candles_5m, signal_candles


def _find_exec_start_index(exec_candles_5m: List[Dict], entry_close_dt: datetime) -> int:
    """
    First raw 5m candle available after signal close.

    If a 15m signal candle closes at 10:15, the first 5m execution candle is
    the 10:15–10:20 candle, whose DB `dt` is 10:15.
    """
    target = _ist_str(entry_close_dt)
    lo, hi = 0, len(exec_candles_5m)
    while lo < hi:
        mid = (lo + hi) // 2
        if exec_candles_5m[mid]["dt"] < target:
            lo = mid + 1
        else:
            hi = mid
    return lo


def _resolve_trade_on_5m(
    exec_candles_5m: List[Dict],
    start_idx: int,
    direction: str,
    entry: float,
    sl: float,
    tp: float,
) -> Optional[Dict[str, Any]]:
    """
    Resolve TP/SL on raw 5m candles.

    Why this exists:
      • The TradingView indicator calculates signals on sigTF.
      • But once inTrade is true, its TP/SL check uses chart high/low/open.
      • Your DB minimum chart/execution granularity is 5m.

    So Python should calculate signals on the requested TF, but resolve exits on
    5m candles. This is the key fix for TradingView cross-verification.
    """
    is_long = direction == "BUY"

    for j in range(start_idx, len(exec_candles_5m)):
        bar = exec_candles_5m[j]
        o, h, l = bar["o"], bar["h"], bar["l"]

        tp_hit = (h >= tp) if is_long else (l <= tp)
        sl_hit = (l <= sl) if is_long else (h >= sl)

        if not (tp_hit or sl_hit):
            continue

        # Same as your Pine default: Conservative (SL first)
        if tp_hit and sl_hit:
            outcome = "SL"
        elif tp_hit:
            outcome = "TP"
        else:
            outcome = "SL"

        # Same gap-exit model as Pine
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

        return {
            "exit_bar": bar["dt"],
            "exit_px": exit_px,
            "outcome": outcome,
            "pnl": pnl,
            "exit_exec_index": j,
        }

    return None


def run_strategy(
    signal_candles: List[Dict],
    exec_candles_5m: List[Dict],
    signal_tf_min: int,
    window_start_ist: Optional[str] = None,
    include_open_trade: bool = False,
) -> List[Dict[str, Any]]:
    """
    Run Strategy 1 (EMA Pullback) with Pine-faithful structure.

    Parameters
    ----------
    signal_candles:
        Aggregated OHLC bars for the chosen signal timeframe. EMA/ATR/signal
        conditions are calculated on these candles.
    exec_candles_5m:
        Raw 5-minute candles used to resolve TP/SL after entry.
    signal_tf_min:
        Signal timeframe in minutes.
    window_start_ist:
        Only record trades whose ENTRY CLOSE time >= this IST string.
    include_open_trade:
        If True, test mode prints the final active/unresolved trade as OPEN.
        Backtest mode should keep False so statistics use completed trades only.

    Returns
    -------
    List of trade dicts. Backtest mode receives completed trades only.
    """
    n = len(signal_candles)
    if n < EMA_SLOW_LEN + ATR_LEN + 10:
        print(f"  ✗ Only {n} signal candles — need ≥ {EMA_SLOW_LEN + ATR_LEN + 10} "
              f"for EMA{EMA_SLOW_LEN} + ATR{ATR_LEN} warm-up.")
        return []

    if not exec_candles_5m:
        print("  ✗ No 5-min execution candles loaded.")
        return []

    closes = [c["c"] for c in signal_candles]
    highs  = [c["h"] for c in signal_candles]
    lows   = [c["l"] for c in signal_candles]
    opens  = [c["o"] for c in signal_candles]

    ema50  = compute_ema(closes, EMA_FAST_LEN)
    ema200 = compute_ema(closes, EMA_SLOW_LEN)
    atr14  = compute_atr(highs, lows, closes, ATR_LEN)

    trades: List[Dict[str, Any]] = []

    # One-trade-at-a-time lock. While the previous trade is active, ignore new signals.
    locked_until_exit_dt: Optional[datetime] = None

    for i in range(1, n):
        if atr14[i] is None:
            continue

        sig_bar = signal_candles[i]
        sig_open_dt = _parse_ist(sig_bar["dt"])
        sig_close_dt = sig_open_dt + timedelta(minutes=signal_tf_min)
        sig_close_str = _ist_str(sig_close_dt)

        # Previous trade still open through this signal close -> Pine would not enter.
        if locked_until_exit_dt is not None and sig_close_dt < locked_until_exit_dt:
            continue

        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        e50 = ema50[i]
        e200 = ema200[i]
        atr_val = atr14[i]

        # Strategy 1 conditions: exact mirror of Pine s1Long/s1Short
        trend_up   = c > e200
        trend_down = c < e200

        long_sig  = trend_up   and (l <= e50) and (c > e50) and (c > o)
        short_sig = trend_down and (h >= e50) and (c < e50) and (c < o)

        if not (long_sig or short_sig):
            continue

        direction = "BUY" if long_sig else "SELL"
        entry = c  # Pine Entry Price Model: Signal close

        if direction == "BUY":
            sl = entry - ATR_MULT * atr_val
            tp = entry + RR_RATIO * (entry - sl)
        else:
            sl = entry + ATR_MULT * atr_val
            tp = entry - RR_RATIO * (sl - entry)

        # Pine uses syminfo.mintick, not a hard 0.05 filter.
        if abs(entry - sl) <= SYMBOL_MINTICK:
            continue

        # Window filtering must use the actual entry close time, not signal candle open time.
        if window_start_ist is not None and sig_close_str < window_start_ist:
            continue

        exec_start_idx = _find_exec_start_index(exec_candles_5m, sig_close_dt)
        resolved = _resolve_trade_on_5m(
            exec_candles_5m=exec_candles_5m,
            start_idx=exec_start_idx,
            direction=direction,
            entry=entry,
            sl=sl,
            tp=tp,
        )

        base_trade = {
            "entry_bar":    sig_bar["dt"],
            "entry_at":     sig_close_str,
            "direction":    direction,
            "entry":        round(entry, 2),
            "sl":           round(sl, 2),
            "tp":           round(tp, 2),
            "ema50":        round(e50, 2),
            "ema200":       round(e200, 2),
            "atr":          round(atr_val, 2),
            "entry_close_hour": sig_close_dt.hour,
        }

        if resolved is None:
            # Active trade at the end of available data. Useful in test mode only.
            if include_open_trade:
                t = dict(base_trade)
                t.update({
                    "exit_bar": "OPEN",
                    "exit_px": None,
                    "outcome": "OPEN",
                    "pnl": 0.0,
                })
                trades.append(t)
            break

        exit_dt = _parse_ist(resolved["exit_bar"])
        locked_until_exit_dt = exit_dt

        t = dict(base_trade)
        t.update({
            "exit_bar": resolved["exit_bar"],
            "exit_px": round(resolved["exit_px"], 2),
            "outcome": resolved["outcome"],
            "pnl": round(resolved["pnl"], 2),
        })
        trades.append(t)

    return trades


# =====================================================================
# HELPERS — data window calculation
# =====================================================================

def _now_ist() -> datetime:
    return datetime.now(IST)


def _compute_data_window(
    backtest_duration: timedelta,
    tf_min: int,
) -> str:
    """
    We need enough warm-up bars BEFORE the backtest window to converge
    EMA200.  Rule of thumb: 3×EMA_SLOW_LEN bars of warm-up.
    Returns the earliest IST datetime string we should load from DB.
    """
    warmup_bars = 3 * EMA_SLOW_LEN          # ~600 bars
    warmup_minutes = warmup_bars * tf_min    # e.g. 600×5 = 3000 min ≈ 2 days for 5m
    # Add generous buffer for weekends / holidays
    warmup_cal = timedelta(minutes=warmup_minutes * 2)
    total_lookback = backtest_duration + warmup_cal
    return _ist_str(_now_ist() - total_lookback)


# =====================================================================
# OUTPUT FORMATTING
# =====================================================================

RESET = "\033[0m"
BOLD  = "\033[1m"
GREEN = "\033[92m"
RED   = "\033[91m"
CYAN  = "\033[96m"
YELLOW= "\033[93m"
DIM   = "\033[2m"


def _color(text: str, code: str) -> str:
    return f"{code}{text}{RESET}"


def _duration_str(entry_dt: str, exit_dt: str) -> str:
    fmt = "%Y-%m-%d %H:%M:%S"
    delta = datetime.strptime(exit_dt, fmt) - datetime.strptime(entry_dt, fmt)
    total_min = int(delta.total_seconds() / 60)
    if total_min < 60:
        return f"{total_min}m"
    h, m = divmod(total_min, 60)
    if h < 24:
        return f"{h}h {m}m"
    d, h = divmod(h, 24)
    return f"{d}d {h}h {m}m"


def print_trade_detail(idx: int, t: Dict, tf_min: int) -> None:
    """Pretty-print one trade for test mode."""
    is_long = t["direction"] == "BUY"
    dir_color = GREEN if is_long else RED
    is_open = t.get("outcome") == "OPEN"
    out_color = YELLOW if is_open else (GREEN if t["outcome"] == "TP" else RED)
    out_icon  = "⏳" if is_open else ("✅" if t["outcome"] == "TP" else "❌")

    sl_dist = abs(t["entry"] - t["sl"])
    tp_dist = abs(t["tp"] - t["entry"])

    print(f"\n  {_color(f'Setup #{idx}', BOLD)}")
    print(f"  {'─'*52}")
    print(f"  Signal Candle  :  {t['entry_bar']} IST")
    print(f"  Entry At       :  {t['entry_at']} IST  (candle close)")
    print(f"  Direction      :  {_color(t['direction'], dir_color)}")
    print(f"  Entry          :  {t['entry']:.2f}")
    print(f"  SL             :  {t['sl']:.2f}   ({'-' if is_long else '+'}{sl_dist:.2f})")
    print(f"  TP             :  {t['tp']:.2f}   ({'+'if is_long else '-'}{tp_dist:.2f})")
    print(f"  ATR(14)        :  {t['atr']:.2f}")
    print(f"  EMA50          :  {t['ema50']:.2f}")
    print(f"  EMA200         :  {t['ema200']:.2f}")
    if is_open:
        print(f"  Result         :  {out_icon}  {_color('OPEN / ACTIVE', out_color)}")
        print(f"  Exit Price     :  --")
        print(f"  Exit Bar       :  --")
        print(f"  PnL            :  --")
        print(f"  Duration       :  active")
    else:
        print(f"  Result         :  {out_icon}  {_color(t['outcome'] + ' HIT', out_color)}")
        print(f"  Exit Price     :  {t['exit_px']:.2f}")
        print(f"  Exit Bar       :  {t['exit_bar']} IST")
        pnl_val = t["pnl"]
        print(f"  PnL            :  {_color(f'{pnl_val:+.2f}', out_color)}")
        print(f"  Duration       :  {_duration_str(t['entry_bar'], t['exit_bar'])}")
    print(f"  {'─'*52}")


def print_test_summary(trades: List[Dict]) -> None:
    completed = [t for t in trades if t.get("outcome") in ("TP", "SL")]
    open_count = sum(1 for t in trades if t.get("outcome") == "OPEN")
    wins   = sum(1 for t in completed if t["outcome"] == "TP")
    losses = sum(1 for t in completed if t["outcome"] == "SL")
    total  = len(completed)
    wr     = (wins / total * 100) if total else 0
    total_pnl = sum(t["pnl"] for t in trades)

    print(f"\n{'═'*64}")
    w_str = _color(f"{wins}W", GREEN)
    l_str = _color(f"{losses}L", RED)
    wr_c  = GREEN if wr >= 50 else RED
    open_note = f"  |  {open_count} OPEN" if open_count else ""
    print(f"  SUMMARY:  {total} completed trades  |  {w_str} / {l_str}  |  "
          f"Win Rate: {_color(f'{wr:.1f}%', wr_c)}  |  "
          f"PnL: {_color(f'{total_pnl:+.2f}', GREEN if total_pnl >= 0 else RED)}" + open_note)
    print(f"{'═'*64}")


def print_hourly_table(trades: List[Dict], tf_min: int) -> None:
    """Hourly accuracy breakdown, sorted by win rate descending."""

    # Bucket by entry candle CLOSE hour (IST). Completed trades only.
    trades = [t for t in trades if t.get("outcome") in ("TP", "SL")]
    buckets: Dict[int, List[Dict]] = {h: [] for h in range(24)}
    for t in trades:
        buckets[t["entry_close_hour"]].append(t)

    rows = []
    for hour in range(24):
        tlist = buckets[hour]
        if not tlist:
            continue
        total  = len(tlist)
        wins   = sum(1 for t in tlist if t["outcome"] == "TP")
        losses = total - wins
        wr     = wins / total * 100
        avg_pnl   = sum(t["pnl"] for t in tlist) / total
        total_pnl = sum(t["pnl"] for t in tlist)
        avg_win   = (sum(t["pnl"] for t in tlist if t["outcome"] == "TP") / wins
                     if wins else 0)
        avg_loss  = (sum(t["pnl"] for t in tlist if t["outcome"] == "SL") / losses
                     if losses else 0)
        buys  = sum(1 for t in tlist if t["direction"] == "BUY")
        sells = total - buys
        rows.append({
            "hour": hour, "total": total, "wins": wins, "losses": losses,
            "wr": wr, "avg_pnl": avg_pnl, "total_pnl": total_pnl,
            "avg_win": avg_win, "avg_loss": avg_loss,
            "buys": buys, "sells": sells,
        })

    rows.sort(key=lambda r: r["wr"], reverse=True)

    # Header
    hdr = (f"  {'Hour (IST)':>13}  │ {'Trades':>6} │ {'Wins':>4} │ {'Loss':>4} │ "
           f"{'Win%':>6} │ {'AvgWin':>8} │ {'AvgLoss':>8} │ {'AvgPnL':>8} │ "
           f"{'TotalPnL':>9} │ {'B/S':>5}")
    sep = f"  {'─'*13}──┼{'─'*8}┼{'─'*6}┼{'─'*6}┼{'─'*8}┼{'─'*10}┼{'─'*10}┼{'─'*10}┼{'─'*11}┼{'─'*6}"

    print(f"\n  {_color('HOURLY ACCURACY  (sorted by win rate)', BOLD)}")
    print(f"  {DIM}Entry hour = IST hour when signal candle CLOSED{RESET}\n")
    print(hdr)
    print(sep)

    for r in rows:
        wr_c = GREEN if r["wr"] >= 50 else RED
        pnl_c = GREEN if r["total_pnl"] >= 0 else RED
        h = r["hour"]
        label = f"{h:02d}:00–{h:02d}:59"
        wr_val = r["wr"]
        aw_val = r["avg_win"]
        al_val = r["avg_loss"]
        tp_val = r["total_pnl"]
        print(f"  {label:>13}  │ {r['total']:>6} │ "
              f"{_color(str(r['wins']), GREEN):>13} │ "
              f"{_color(str(r['losses']), RED):>13} │ "
              f"{_color(f'{wr_val:5.1f}%', wr_c):>15} │ "
              f"{_color(f'{aw_val:+7.2f}', GREEN):>17} │ "
              f"{_color(f'{al_val:+7.2f}', RED):>17} │ "
              f"{r['avg_pnl']:+8.2f} │ "
              f"{_color(f'{tp_val:+9.2f}', pnl_c):>20} │ "
              f"{r['buys']}B/{r['sells']}S")

    print(sep)


def print_direction_split(trades: List[Dict]) -> None:
    trades = [t for t in trades if t.get("outcome") in ("TP", "SL")]
    buys  = [t for t in trades if t["direction"] == "BUY"]
    sells = [t for t in trades if t["direction"] == "SELL"]

    def _stats(tlist, label):
        n = len(tlist)
        if n == 0:
            print(f"  {label:>6}:  no trades")
            return
        w = sum(1 for t in tlist if t["outcome"] == "TP")
        wr = w / n * 100
        pnl = sum(t["pnl"] for t in tlist)
        wr_c = GREEN if wr >= 50 else RED
        pnl_c = GREEN if pnl >= 0 else RED
        print(f"  {label:>6}:  {n} trades  |  {w}W / {n-w}L  |  "
              f"WR {_color(f'{wr:.1f}%', wr_c)}  |  "
              f"PnL {_color(f'{pnl:+.2f}', pnl_c)}")

    print(f"\n  {_color('BY DIRECTION', BOLD)}")
    _stats(buys,  "BUY")
    _stats(sells, "SELL")


def print_overall_stats(trades: List[Dict]) -> None:
    if not trades:
        print("\n  No trades to summarize.\n")
        return

    total = len(trades)
    wins  = sum(1 for t in trades if t["outcome"] == "TP")
    losses = total - wins
    wr = wins / total * 100

    total_pnl = sum(t["pnl"] for t in trades)
    gross_win = sum(t["pnl"] for t in trades if t["outcome"] == "TP")
    gross_loss = abs(sum(t["pnl"] for t in trades if t["outcome"] == "SL"))
    pf = (gross_win / gross_loss) if gross_loss > 0 else float("inf")

    pnl_list = [t["pnl"] for t in trades]
    best  = max(pnl_list)
    worst = min(pnl_list)
    avg   = total_pnl / total

    # Streaks
    max_w = max_l = cur_w = cur_l = 0
    for t in trades:
        if t["outcome"] == "TP":
            cur_w += 1; cur_l = 0
            max_w = max(max_w, cur_w)
        else:
            cur_l += 1; cur_w = 0
            max_l = max(max_l, cur_l)

    wr_c  = GREEN if wr >= 50 else RED
    pnl_c = GREEN if total_pnl >= 0 else RED

    print(f"\n{'═'*72}")
    print(f"  {_color('OVERALL STATS', BOLD)}")
    print(f"  {'─'*52}")
    print(f"  Total Trades   :  {total}")
    print(f"  Winners        :  {_color(str(wins), GREEN)}")
    print(f"  Losers         :  {_color(str(losses), RED)}")
    print(f"  Win Rate       :  {_color(f'{wr:.1f}%', wr_c)}")
    print(f"  Total PnL      :  {_color(f'{total_pnl:+.2f}', pnl_c)}")
    print(f"  Profit Factor  :  {pf:.2f}")
    print(f"  Avg Trade      :  {avg:+.2f}")
    print(f"  Best Trade     :  {_color(f'{best:+.2f}', GREEN)}")
    print(f"  Worst Trade    :  {_color(f'{worst:+.2f}', RED)}")
    print(f"  Max Win Streak :  {max_w}")
    print(f"  Max Loss Streak:  {max_l}")

    # Date range of trades
    first_entry = trades[0]["entry_bar"]
    last_entry  = trades[-1]["entry_bar"]
    print(f"  First Entry    :  {first_entry} IST")
    print(f"  Last Entry     :  {last_entry} IST")
    print(f"{'═'*72}\n")


# =====================================================================
# TEST COMMAND
# =====================================================================

def cmd_test(days: int, tf_min: int) -> None:
    conn = init_db()
    total_candles = count_candles(conn)
    if total_candles == 0:
        print("  ✗ Database is empty. Run 'seed' first.")
        conn.close()
        return

    now = _now_ist()
    bt_start = now - timedelta(days=days)
    data_start_str = _compute_data_window(timedelta(days=days), tf_min)
    bt_start_str   = _ist_str(bt_start)

    print(f"\n{'═'*64}")
    print(f"  {_color('XAUUSD EMA Pullback — TEST MODE', BOLD)}")
    print(f"  Past {days} day(s)  |  TF: {tf_min}min  |  Strategy 1")
    print(f"  Backtest window : {bt_start_str}  →  now")
    print(f"  Data loaded from: {data_start_str}  (for EMA warm-up)")
    print(f"{'═'*64}")

    candles_5m = load_candles(conn, start_ist=data_start_str)
    conn.close()
    print(f"  Loaded {len(candles_5m):,} raw 5-min candles from DB")

    candles_5m, signal_candles = prepare_strategy_candles(candles_5m, tf_min)
    print(f"  Prepared {len(candles_5m):,} execution candles after market/closed-bar filters")
    print(f"  Aggregated to {len(signal_candles):,} signal candles @ {tf_min}min")
    print("  Exit resolution: raw 5-min candles, to match TradingView chart-bar TP/SL checks")

    trades = run_strategy(
        signal_candles=signal_candles,
        exec_candles_5m=candles_5m,
        signal_tf_min=tf_min,
        window_start_ist=bt_start_str,
        include_open_trade=True,
    )
    print(f"  Found {_color(str(len(trades)), CYAN)} setup(s) in window")

    for idx, t in enumerate(trades, 1):
        print_trade_detail(idx, t, tf_min)

    print_test_summary(trades)


# =====================================================================
# BACKTEST COMMAND
# =====================================================================

def cmd_backtest(months: int, tf_min: int) -> None:
    conn = init_db()
    total_candles = count_candles(conn)
    if total_candles == 0:
        print("  ✗ Database is empty. Run 'seed' first.")
        conn.close()
        return

    now = _now_ist()
    bt_start = now - timedelta(days=months * 30)  # approximate months
    data_start_str = _compute_data_window(timedelta(days=months * 30), tf_min)
    bt_start_str   = _ist_str(bt_start)

    print(f"\n{'═'*72}")
    print(f"  {_color('XAUUSD EMA Pullback — BACKTEST', BOLD)}")
    print(f"  Past {months} month(s)  |  TF: {tf_min}min  |  Strategy 1  |  RR 1:{RR_RATIO:g}")
    print(f"  Backtest window : {bt_start_str}  →  now")
    print(f"  Data loaded from: {data_start_str}  (for EMA warm-up)")
    print(f"  SL: {ATR_MULT}× ATR({ATR_LEN})   |   Entry: signal candle close")
    print(f"  Same-bar conflict: Conservative (SL first)")
    print(f"{'═'*72}")

    candles_5m = load_candles(conn, start_ist=data_start_str)
    conn.close()
    print(f"  Loaded {len(candles_5m):,} raw 5-min candles from DB")

    candles_5m, signal_candles = prepare_strategy_candles(candles_5m, tf_min)
    print(f"  Prepared {len(candles_5m):,} execution candles after market/closed-bar filters")
    print(f"  Aggregated to {len(signal_candles):,} signal candles @ {tf_min}min")
    print("  Exit resolution: raw 5-min candles, to match TradingView chart-bar TP/SL checks")

    trades = run_strategy(
        signal_candles=signal_candles,
        exec_candles_5m=candles_5m,
        signal_tf_min=tf_min,
        window_start_ist=bt_start_str,
        include_open_trade=False,
    )
    print(f"  Completed trades in window: {_color(str(len(trades)), CYAN)}")

    if not trades:
        print("\n  No trades found in the backtest window.")
        return

    print_hourly_table(trades, tf_min)
    print_direction_split(trades)
    print_overall_stats(trades)


# =====================================================================
# DB CLEAN COMMAND
# =====================================================================

def cmd_clean_bad_data(reset_live_signals: bool = False) -> None:
    """Remove obvious weekend/invalid rows that may already exist in data.db."""
    conn = init_db()

    candle_rows = conn.execute("SELECT dt, o, h, l, c FROM candles_5m ORDER BY dt ASC").fetchall()
    bad_candle_dts = []
    for dt, o, h, l, c in candle_rows:
        row = {"dt": dt, "o": o, "h": h, "l": l, "c": c}
        if not is_valid_5m_candle_row(row):
            bad_candle_dts.append(dt)

    if bad_candle_dts:
        conn.executemany("DELETE FROM candles_5m WHERE dt = ?", [(dt,) for dt in bad_candle_dts])

    if reset_live_signals:
        deleted_live = conn.execute("DELETE FROM live_signals").rowcount
    else:
        signal_rows = conn.execute("SELECT id, entry_at FROM live_signals").fetchall()
        bad_signal_ids = [sid for sid, entry_at in signal_rows if not is_valid_xau_market_time_ist(entry_at)]
        deleted_live = len(bad_signal_ids)
        if bad_signal_ids:
            conn.executemany("DELETE FROM live_signals WHERE id = ?", [(sid,) for sid in bad_signal_ids])

    conn.commit()
    total_after = count_candles(conn)
    mn, mx = get_range(conn)
    conn.close()

    print(f"\n{'═'*64}")
    print(f"  {_color('DB CLEAN COMPLETE', BOLD)}")
    print(f"  Removed candles     : {len(bad_candle_dts):,}")
    print(f"  Removed live signals: {deleted_live:,}")
    print(f"  Candles remaining   : {total_after:,}")
    if mn and mx:
        print(f"  Range               : {mn} → {mx} IST")
    print(f"{'═'*64}\n")


# =====================================================================
# LIVE COMMAND
# =====================================================================

def _hhmm_to_minutes(value: str) -> int:
    hh, mm = value.split(":", 1)
    return int(hh) * 60 + int(mm)


def _entry_in_windows(entry_at: str, windows: List[Dict[str, str]]) -> bool:
    dt = _parse_ist(entry_at)
    m = dt.hour * 60 + dt.minute
    for w in windows:
        start = _hhmm_to_minutes(w["start"])
        end = _hhmm_to_minutes(w["end"])
        if start < end:
            if start <= m < end:
                return True
        else:
            # Overnight window support, e.g. 22:00 → 02:00
            if m >= start or m < end:
                return True
    return False


def _windows_label(windows: List[Dict[str, str]]) -> str:
    return ", ".join(f"{w['start']}–{w['end']}" for w in windows)


def _price_diff(entry: float, price: float) -> float:
    return abs(float(price) - float(entry))


def _fmt_price(value: Any) -> str:
    return f"{float(value):.2f}"

def _active_trade_one_line(trade: Dict[str, Any]) -> str:
    return (
        f"ACTIVE {trade['tf_min']}m {trade['direction']} | "
        f"entry_at {trade['entry_at']} IST | "
        f"entry {_fmt_price(trade['entry'])} | "
        f"SL {_fmt_price(trade['sl'])} | "
        f"TP {_fmt_price(trade['tp'])}"
    )


def _strategy_label(rule: Dict[str, Any]) -> str:
    if rule.get("strategy") == "ema_pullback":
        return "EMA Pullback"
    return str(rule.get("strategy", "Unknown Strategy"))


def _print_live_signal(rule: Dict[str, Any], trade: Dict[str, Any]) -> None:
    entry = float(trade["entry"])
    sl = float(trade["sl"])
    tp = float(trade["tp"])

    print(f"\n{'═'*72}")
    print(f"  {_color('LIVE SETUP FORMED', BOLD)}")
    print(f"  Formed     : {trade['entry_at']} IST")
    print(f"  Timeframe  : {rule['tf_min']}m")
    print(f"  Strategy   : {_strategy_label(rule)}")
    print(f"  Direction  : {trade['direction']}")
    print(f"  Entry      : {_fmt_price(entry)}")
    print(f"  SL         : {_fmt_price(sl)} ({_price_diff(entry, sl):.2f})")
    print(f"  TP         : {_fmt_price(tp)} ({_price_diff(entry, tp):.2f})")
    print(f"  Status     : OPEN, timeframe locked")
    print(f"{'═'*72}\n")


def _print_live_exit(trade: Dict[str, Any]) -> None:
    print(f"\n{'═'*72}")
    print(f"  {_color('LIVE TRADE CLOSED', BOLD)}")
    print(f"  Result     : {trade['outcome']} HIT")
    print(f"  Timeframe  : {trade['tf_min']}m")
    print(f"  Direction  : {trade['direction']}")
    print(f"  Entry Time : {trade['entry_at']} IST")
    print(f"  Entry      : {_fmt_price(trade['entry'])}")
    print(f"  Exit Price : {_fmt_price(trade['exit_px'])}")
    print(f"  Exit Bar   : {trade['exit_bar']} IST")
    print(f"  PnL        : {float(trade['pnl']):+.2f}")
    print(f"  Status     : timeframe unlocked")
    print(f"{'═'*72}\n")


def _send_alert(subject: str, body: str, *, tags: str = "warning") -> None:
    """
    Sends:
      1. ntfy push notification first
      2. email second

    Both errors are swallowed so main live code never breaks.
    """

    # =========================
    # HARD-CODE CONFIG HERE
    # =========================
    SENDER_EMAIL = "shubhamapcollege@gmail.com"
    RECEIVER_EMAIL = "shubhamapstudy23@gmail.com"
    GMAIL_APP_PASSWORD = "hjhkioaqktabmxhu"

    NTFY_URL = "https://ntfy.sh/xaualertvpsshubham23"

    # =========================
    # 1) SEND NTFY PUSH FIRST
    # =========================
    try:
        req = urllib.request.Request(
            NTFY_URL,
            data=body.encode("utf-8"),
            method="POST",
            headers={
                "Title": subject,
                "Priority": "urgent",
                "Tags": tags,
            },
        )
        urllib.request.urlopen(req, timeout=10).read()
        print("  ✓ ntfy push alert sent")
    except Exception as e:
        print(f"  ⚠ ntfy alert failed, continuing anyway: {e}")

    # =========================
    # 2) SEND EMAIL SECOND
    # =========================
    try:
        msg = EmailMessage()
        msg["From"] = SENDER_EMAIL
        msg["To"] = RECEIVER_EMAIL
        msg["Subject"] = subject
        msg.set_content(body)

        with smtplib.SMTP("smtp.gmail.com", 587, timeout=20) as smtp:
            smtp.starttls()
            smtp.login(SENDER_EMAIL, GMAIL_APP_PASSWORD.replace(" ", ""))
            smtp.send_message(msg)

        print("  ✓ email alert sent")
    except Exception as e:
        print(f"  ⚠ email alert failed, continuing anyway: {e}")


def send_trade_setup_alert(rule: Dict[str, Any], trade: Dict[str, Any]) -> None:
    entry = float(trade["entry"])
    sl = float(trade["sl"])
    tp = float(trade["tp"])

    subject = f"XAUUSD {trade['direction']} setup | {rule['tf_min']}m | {trade['entry_at']} IST"

    body = f"""XAUUSD Setup Formed

Formed: {trade['entry_at']} IST
Timeframe: {rule['tf_min']}m
Strategy: {_strategy_label(rule)}
Direction: {trade['direction']}

Entry: {_fmt_price(entry)}
SL: {_fmt_price(sl)} ({_price_diff(entry, sl):.2f})
TP: {_fmt_price(tp)} ({_price_diff(entry, tp):.2f})
"""

    _send_alert(subject, body, tags="warning")


def send_trade_exit_alert(trade: Dict[str, Any]) -> None:
    subject = (
        f"XAUUSD {trade['outcome']} hit | "
        f"{trade['tf_min']}m {trade['direction']} | "
        f"entry {trade['entry_at']} IST"
    )

    body = f"""XAUUSD Trade Closed

Result: {trade['outcome']} HIT
Timeframe: {trade['tf_min']}m
Direction: {trade['direction']}

Entry Time: {trade['entry_at']} IST
Entry: {_fmt_price(trade['entry'])}

Exit Bar: {trade['exit_bar']} IST
Exit Price: {_fmt_price(trade['exit_px'])}
PnL: {float(trade['pnl']):+.2f}
"""

    tags = "white_check_mark" if trade["outcome"] == "TP" else "x"
    _send_alert(subject, body, tags=tags)

def evaluate_live_rules(conn: sqlite3.Connection) -> int:
    """
    Live engine with timeframe-specific locks and silent outside-window locks.

    Correct behavior:
      • Strategy state runs all day per timeframe.
      • Rule windows only decide whether to alert.
      • Outside-window trades still lock the timeframe.
      • 5m lock does not block 15m.
      • If TP/SL hits, unlock and then check latest candle for a new setup.
    """
    event_count = 0
    now = _now_ist()

    latest_db = get_latest_candle_dt(conn)
    if not latest_db:
        print(f"  No candles available for live evaluation at {_ist_str(now)} IST")
        return 0

    latest_exec_close = _parse_ist(latest_db) + timedelta(minutes=5)

    rules_to_check = LIVE_RULES if LIVE_RULES else [{
        "name": "S1_EMA_PULLBACK_ANY_SETUP",
        "enabled": True,
        "tf_min": 5,
        "windows": [{"start": "00:00", "end": "00:00"}],
        "strategy": "ema_pullback",
    }]

    rules_by_tf: Dict[int, List[Dict[str, Any]]] = {}

    for rule in rules_to_check:
        if not rule.get("enabled", False):
            continue

        if rule.get("strategy") != "ema_pullback":
            print(f"  ⚠ Unsupported live strategy in rule {rule.get('name')}: {rule.get('strategy')}")
            continue

        tf_min = int(rule["tf_min"])
        rules_by_tf.setdefault(tf_min, []).append(rule)

    if not rules_by_tf:
        print(f"  No enabled live rules at {_ist_str(now)} IST")
        return 0

    for tf_min, tf_rules in rules_by_tf.items():
        data_start_str = _compute_data_window(timedelta(days=LIVE_LOOKBACK_DAYS), tf_min)

        candles_5m = load_candles(conn, start_ist=data_start_str)
        candles_5m, signal_candles = prepare_strategy_candles(
            candles_5m,
            tf_min,
            verbose=False,
        )

        if not candles_5m or not signal_candles:
            print(f"  No candles available for live TF {tf_min}m")
            continue

        # -------------------------------------------------------------
        # A) First check existing timeframe lock from DB.
        # -------------------------------------------------------------
        just_closed_exit_bar: Optional[str] = None

        open_trade = get_open_live_trade_for_tf(conn, tf_min)

        if open_trade:
            resolved = resolve_open_live_trade_on_5m(open_trade, candles_5m)

            if resolved is None:
                print(
                    f"  TF {tf_min}m locked: OPEN {open_trade['direction']} "
                    f"from {open_trade['entry_at']} IST has not hit TP/SL yet | "
                    f"{_active_trade_one_line(open_trade)}"
                )
                continue

            just_closed_exit_bar = resolved["exit_bar"]

            close_live_signal(conn, int(open_trade["id"]), resolved)

            closed_trade = dict(open_trade)
            closed_trade.update({
                "outcome": resolved["outcome"],
                "exit_bar": resolved["exit_bar"],
                "exit_px": round(float(resolved["exit_px"]), 2),
                "pnl": round(float(resolved["pnl"]), 2),
            })

            if int(open_trade.get("is_silent_lock", 0)) == 0:
                _print_live_exit(closed_trade)
                send_trade_exit_alert(closed_trade)
                mark_live_exit_notified(conn, int(open_trade["id"]))
                event_count += 1
            else:
                print(
                    f"  TF {tf_min}m silent lock closed: "
                    f"{closed_trade['outcome']} at {closed_trade['exit_bar']} IST"
                )

            # Do not continue.
            # Timeframe is now unlocked, so below we check whether a new
            # setup formed on the latest confirmed candle.

        # -------------------------------------------------------------
        # B) Run raw strategy for this timeframe with NO window filter.
        #    This is what keeps live mode aligned with backtest mode.
        # -------------------------------------------------------------
        raw_trades = run_strategy(
            signal_candles=signal_candles,
            exec_candles_5m=candles_5m,
            signal_tf_min=tf_min,
            window_start_ist=None,
            include_open_trade=True,
        )

        raw_open_trade = raw_trades[-1] if raw_trades and raw_trades[-1].get("outcome") == "OPEN" else None

        if raw_open_trade is None:
            continue

        latest_signal_close_dt = _candle_close_dt(signal_candles[-1]["dt"], tf_min)
        latest_signal_close_str = _ist_str(latest_signal_close_dt)

        matching_rule = _trade_matches_any_rule_window(raw_open_trade, tf_rules)

        # -------------------------------------------------------------
        # C) If raw strategy says a trade is open but it is outside your
        #    alert windows, create a silent lock.
        # -------------------------------------------------------------
        if matching_rule is None:
            if record_live_signal(
                conn,
                f"SILENT_TF_LOCK_{tf_min}M",
                tf_min,
                raw_open_trade,
                silent_lock=True,
            ):
                print(
                    f"  TF {tf_min}m silent lock created: "
                    f"{raw_open_trade['direction']} from {raw_open_trade['entry_at']} IST "
                    f"outside alert windows"
                )
            continue

        # -------------------------------------------------------------
        # D) If the open raw trade is older than the latest signal close,
        #    normally it is a stale/catch-up trade and should be silent.
        #
        #    Exception:
        #    If this timeframe's previous OPEN trade closed in this same
        #    evaluation cycle, then a new setup at/after that exit bar is
        #    allowed to alert if it is still recent enough.
        # -------------------------------------------------------------
        is_latest_signal = raw_open_trade["entry_at"] == latest_signal_close_str

        is_post_unlock_setup = _is_post_unlock_setup_alertable(
            raw_open_trade,
            just_closed_exit_bar,
            latest_signal_close_str,
            tf_min,
        )

        if not is_latest_signal and not is_post_unlock_setup:
            if record_live_signal(
                conn,
                f"SILENT_TF_LOCK_{tf_min}M",
                tf_min,
                raw_open_trade,
                silent_lock=True,
            ):
                print(
                    f"  TF {tf_min}m silent lock created for older active trade: "
                    f"{raw_open_trade['direction']} from {raw_open_trade['entry_at']} IST"
                )
            continue

        # -------------------------------------------------------------
        # E) Latest candle formed a valid setup inside one of your windows.
        #    Alert it and lock this timeframe.
        # -------------------------------------------------------------
        if record_live_signal(
            conn,
            matching_rule["name"],
            tf_min,
            raw_open_trade,
            silent_lock=False,
        ):
            if is_post_unlock_setup and not is_latest_signal:
                print(
                    f"  TF {tf_min}m post-unlock setup allowed: "
                    f"{raw_open_trade['direction']} from {raw_open_trade['entry_at']} IST "
                    f"after previous trade closed at {just_closed_exit_bar} IST"
                )

            _print_live_signal(matching_rule, raw_open_trade)
            send_trade_setup_alert(matching_rule, raw_open_trade)
            event_count += 1

    if event_count == 0:
        print(f"  No new live alert at {_ist_str(now)} IST")

    return event_count


def _next_live_check_time(now: Optional[datetime] = None) -> datetime:
    """
    Next time a newly closed 5m candle should be API-safe.

    Important:
    LIVE_CANDLE_DELAY_SEC is the real provider-availability wait.
    LIVE_SCHEDULER_SAFETY_SEC is only a small buffer to avoid waking just before
    the exact boundary, which can otherwise push processing to the next 5m cycle.
    """
    now = now or _now_ist()

    latest_open = latest_api_eligible_5m_open(now)
    next_open = latest_open + timedelta(minutes=5)

    # next_open candle closes 5 minutes after its open,
    # then we wait provider delay + tiny scheduler safety.
    return next_open + timedelta(
        minutes=5,
        seconds=LIVE_CANDLE_DELAY_SEC + LIVE_SCHEDULER_SAFETY_SEC,
    )

def _sleep_until(target_dt: datetime) -> None:
    """
    Sleep until target_dt has actually passed.

    This avoids int() truncation and protects against waking slightly before
    the candle eligibility boundary.
    """
    while True:
        remaining = (target_dt - _now_ist()).total_seconds()
        if remaining <= 0:
            return

        # Sleep in chunks so system clock drift / PM2 / VPS scheduling does not
        # create one huge blind sleep.
        _time.sleep(min(remaining, 30))

def cmd_live(once: bool = False) -> None:
    if API_KEY == "YOUR_API_KEY_HERE":
        print("✗ Set your Twelve Data API key in the API_KEY variable at the top of the script.")
        return

    conn = init_db()
    pending_opens: set[str] = set()

    print(f"\n{'═'*72}")
    print(f"  {_color('XAUUSD EMA Pullback — LIVE MONITOR', BOLD)}")
    print(f"  Symbol: {SYMBOL}  |  DB: {DB_FILE}")
    print(f"  Candle fetch delay: {LIVE_CANDLE_DELAY_SEC}s after each 5m candle close")
    print(f"  Retry: once after {LIVE_RETRY_DELAY_SEC}s if latest candle is unavailable")
    print(f"  Rules:")
    for r in LIVE_RULES:
        if r.get("enabled"):
            print(f"    • {r['name']} | TF {r['tf_min']}min | windows {_windows_label(r['windows'])} IST")
    print(f"{'═'*72}\n")

    try:
        while True:
            catch_up_db_to_latest_eligible(conn)
            pending_opens = fetch_live_tick_candles(conn, pending_opens)
            evaluate_live_rules(conn)

            if once:
                break

            nxt = _next_live_check_time()
            print(f"  Next live check at {_ist_str(nxt)} IST\n")
            _sleep_until(nxt)
    except KeyboardInterrupt:
        print("\n  Live monitor stopped by user.")
    finally:
        conn.close()


# =====================================================================
# MAIN — CLI entry point
# =====================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="XAUUSD EMA Pullback Backtester",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python xau_backtest.py seed
  python xau_backtest.py test --days 5
  python xau_backtest.py test --days 5 --tf 15
  python xau_backtest.py backtest --months 4 --tf 5
  python xau_backtest.py backtest --months 4 --tf 15
  python xau_backtest.py backtest --months 4 --tf 60
  python xau_backtest.py live
  python xau_backtest.py live --once
  python xau_backtest.py clean
  python xau_backtest.py clean --reset-live-signals
        """,
    )
    sub = parser.add_subparsers(dest="cmd")

    # ── seed ──
    sub.add_parser("seed", help="Download & store 5min candles from 2025-01-01 to now")

    # ── test ──
    p_test = sub.add_parser("test", help="Cross-verification: print all setups in past N days")
    p_test.add_argument("--days", type=int, required=True,
                        help="Look back this many days")
    p_test.add_argument("--tf", type=int, default=5,
                        help="Timeframe in minutes (default: 5)")

    # ── backtest ──
    p_bt = sub.add_parser("backtest", help="Hourly accuracy breakdown for past N months")
    p_bt.add_argument("--months", type=int, required=True,
                      help="Look back this many months")
    p_bt.add_argument("--tf", type=int, default=5,
                      help="Timeframe in minutes (default: 5)")

    # ── clean ──
    p_clean = sub.add_parser("clean", help="Remove obvious weekend/invalid candles already stored in DB")
    p_clean.add_argument("--reset-live-signals", action="store_true",
                         help="Also delete every row from live_signals, not only invalid-weekend ones")

    # ── live ──
    p_live = sub.add_parser("live", help="Catch up DB, poll new 5min candles, and log configured live setups")
    p_live.add_argument("--once", action="store_true",
                        help="Run one catch-up/fetch/evaluate cycle and exit")

    args = parser.parse_args()

    if args.cmd is None:
        parser.print_help()
        sys.exit(0)

    # ── Pre-flight checks ──
    if args.cmd == "seed":
        if API_KEY == "YOUR_API_KEY_HERE":
            print("✗ Set your Twelve Data API key in the API_KEY variable at the top of the script.")
            sys.exit(1)
        print(f"\n{'═'*64}")
        print(f"  {_color('SEEDING DATABASE', BOLD)}")
        print(f"  Symbol: {SYMBOL}  |  Interval: 5min  |  DB: {DB_FILE}")
        print(f"{'═'*64}\n")
        cmd_seed()

    elif args.cmd == "test":
        cmd_test(args.days, args.tf)

    elif args.cmd == "backtest":
        cmd_backtest(args.months, args.tf)

    elif args.cmd == "clean":
        cmd_clean_bad_data(reset_live_signals=args.reset_live_signals)

    elif args.cmd == "live":
        cmd_live(once=args.once)


if __name__ == "__main__":
    main()
