# -*- coding: utf-8 -*-
"""
Created on Tue Jan 27 10:43:26 2026

@author: skell
"""

# pipeline/10_ingest/ingest_ncaa_api_recent.py
#
# Daily incremental ingest for NCAA men's D1:
# - Pulls scoreboard + team-stats (Boxscore schema) for a small sliding window (default: 2 days)
# - Inserts into: teams, games, team_game_stats
# - safe to run daily; skips existing games by (date, home_team, away_team)
#
# Basically just keeps the DB up to date without reprocessing everything
#
# Usage:
#   python ingest_ncaa_api_recent.py
#   python ingest_ncaa_api_recent.py --days 3
#   python ingest_ncaa_api_recent.py --start 2026-01-20 --end 2026-01-27
#
# Notes:
# - Uses retries because this API randomly throws 500s sometimes
# - Does NOT iterate offseason dates (no need)

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))  # allows imports from project root

import argparse
import sqlite3
import requests
import time
import random
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, Tuple

from config import CFG

BASE = "https://ncaa-api.henrygd.me"

# small sleeps so we don't hammer the API (and avoid getting blocked)
SLEEP_SEC_SCOREBOARD = 0.05
SLEEP_SEC_TEAMSTATS  = 0.20

# scoreboard is usually stable, team-stats fails more often
MAX_RETRIES_SCOREBOARD = 2
MAX_RETRIES_TEAMSTATS  = 5


# -------------------------
# SQLite helpers
# -------------------------
def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")  # keep relationships clean
    return conn

def get_or_create_team_id(conn: sqlite3.Connection, team_name: str) -> int:
    team_name = (team_name or "").strip()
    if not team_name:
        raise ValueError("Empty team_name")

    # try to find existing team first
    row = conn.execute("SELECT team_id FROM teams WHERE team_name = ?", (team_name,)).fetchone()
    if row:
        return int(row[0])

    # otherwise insert new team
    cur = conn.execute("INSERT INTO teams (team_name, conference) VALUES (?, NULL)", (team_name,))
    return int(cur.lastrowid)

def game_exists(conn: sqlite3.Connection, game_date: str, home_team_id: int, away_team_id: int) -> Optional[int]:
    row = conn.execute(
        "SELECT game_id FROM games WHERE game_date=? AND home_team_id=? AND away_team_id=?",
        (game_date, home_team_id, away_team_id),
    ).fetchone()
    return int(row[0]) if row else None

def insert_game(conn: sqlite3.Connection,
                ncaa_game_id: str,
                season: int,
                game_date: str,
                home_team_id: int,
                away_team_id: int,
                neutral_site: int,
                home_score: int,
                away_score: int) -> int:
    # simple insert wrapper
    cur = conn.execute(
        """
        INSERT INTO games (
            ncaa_game_id, season, game_date,
            home_team_id, away_team_id, neutral_site,
            home_score, away_score
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (ncaa_game_id, season, game_date, home_team_id, away_team_id, neutral_site, home_score, away_score),
    )
    return int(cur.lastrowid)

def season_label(d: date) -> int:
    # NCAA season labeled by spring end-year (ex: Nov 2025 -> 2026 season)
    return d.year + 1 if d.month >= 11 else d.year


# -------------------------
# HTTP helpers (retries)
# -------------------------
def _get_json(path: str, max_retries: int) -> Dict[str, Any]:
    url = BASE + path
    backoff = 0.6

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=30)

            # treat these as transient failures (common with this API)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"{r.status_code} transient", response=r)

            r.raise_for_status()
            return r.json()

        except Exception:
            if attempt == max_retries:
                raise

            # exponential backoff + slight randomness
            sleep_s = min(6.0, backoff) + random.uniform(0, 0.35)
            time.sleep(sleep_s)
            backoff *= 2.0

def get_scoreboard(d: date) -> Dict[str, Any]:
    return _get_json(f"/scoreboard/basketball-men/d1/{d.year:04d}/{d.month:02d}/{d.day:02d}", MAX_RETRIES_SCOREBOARD)

def get_teamstats(game_id: str) -> Dict[str, Any]:
    return _get_json(f"/game/{game_id}/team-stats", MAX_RETRIES_TEAMSTATS)


# -------------------------
# Math helpers
# -------------------------
def safe_float(x) -> Optional[float]:
    # defensive parsing since API can be inconsistent
    try:
        if x is None:
            return None
        return float(str(x).replace("%", ""))
    except Exception:
        return None

def pct(made: Optional[float], att: Optional[float]) -> Optional[float]:
    if made is None or att is None or att == 0:
        return None
    return float(made / att)

def possessions_estimate(fga: float, fta: float, orb: float, tov: float) -> float:
    # standard tempo estimate
    return float(fga + 0.475 * fta - orb + tov)

def off_reb_pct(orb: Optional[float], opp_drb: Optional[float]) -> Optional[float]:
    if orb is None or opp_drb is None:
        return None
    denom = orb + opp_drb
    if denom == 0:
        return None
    return float(orb / denom)


# -------------------------
# Parse Boxscore schema
# -------------------------
def extract_home_away_from_boxscore_schema(ts: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    teams = ts.get("teams") or []
    if not isinstance(teams, list) or len(teams) < 2:
        raise ValueError("team-stats payload missing teams[]")

    # find home/away flags from API
    home = next((t for t in teams if t.get("isHome") is True), None)
    away = next((t for t in teams if t.get("isHome") is False), None)

    if home is None or away is None:
        raise ValueError("Could not determine home/away from teams[].isHome")

    def norm_team(t: Dict[str, Any]) -> Dict[str, Any]:
        tid = t.get("teamId")
        if tid is None:
            raise ValueError("Missing teamId in teams[]")

        name = (t.get("nameShort") or t.get("nameFull") or t.get("teamName") or "").strip()
        if not name:
            raise ValueError("Missing team name in teams[]")

        return {"teamId": int(str(tid)), "name": name}

    return norm_team(home), norm_team(away)

def extract_team_stats_by_teamid(ts: Dict[str, Any]) -> Dict[int, Dict[str, Optional[float]]]:
    boxes = ts.get("teamBoxscore") or []
    if not isinstance(boxes, list) or len(boxes) < 2:
        raise ValueError("team-stats payload missing teamBoxscore[]")

    out: Dict[int, Dict[str, Optional[float]]] = {}

    for b in boxes:
        tid = b.get("teamId")
        if tid is None:
            continue

        tid_int = int(str(tid))
        stats = b.get("teamStats") or {}

        # pull out only what we care about for now
        fgm = safe_float(stats.get("fieldGoalsMade"))
        fga = safe_float(stats.get("fieldGoalsAttempted"))
        tpm = safe_float(stats.get("threePointsMade"))
        tpa = safe_float(stats.get("threePointsAttempted"))
        ftm = safe_float(stats.get("freeThrowsMade"))
        fta = safe_float(stats.get("freeThrowsAttempted"))
        orb = safe_float(stats.get("offensiveRebounds"))
        trb = safe_float(stats.get("totalRebounds"))
        tov = safe_float(stats.get("turnovers"))
        stl = safe_float(stats.get("steals"))

        drb = (trb - orb) if (trb is not None and orb is not None) else None

        out[tid_int] = {
            "fgm": fgm, "fga": fga,
            "three_pm": tpm, "three_pa": tpa,
            "ftm": ftm, "fta": fta,
            "orb": orb, "drb": drb,
            "tov": tov, "stl": stl,
        }

    return out