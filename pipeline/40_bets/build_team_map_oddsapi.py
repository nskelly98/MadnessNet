# build_team_map_oddsapi.py
#
# Fuzzy-maps OddsAPI team names to my internal NCAA team IDs:
# - Pulls unique team names from odds snapshots
# - Normalizes names to make matching less messy
# - Auto-maps strong matches, flags medium ones for review
# - Leaves weak matches unmapped so they do not silently break stuff
#
# Basically this is the translator between OddsAPI naming and my own team table
#
# Usage:
#   python build_team_map_oddsapi.py
#
# Notes:
# - Only looks at team names that actually show up in the odds feed
# - Does not overwrite manual mappings that already exist
# - Uses RapidFuzz token_set_ratio for fuzzy matching

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))  # allow imports from project root

import re
import sqlite3
import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

from config import CFG


AUTO_THRESHOLD = getattr(CFG, "MAP_AUTO_THRESHOLD", 92)     # strong enough to trust automatically
REVIEW_THRESHOLD = getattr(CFG, "MAP_REVIEW_THRESHOLD", 86) # decent match, but worth eyeballing
BOOK = getattr(CFG, "BOOK", "FanDuel")


def norm(s: str) -> str:
    # normalize names so fuzzy matching has a better shot
    if s is None:
        return ""
    s = s.lower().strip()
    s = s.replace("&", " and ")
    s = re.sub(r"\(.*?\)", " ", s)          # remove parentheticals
    s = re.sub(r"[’'`]", "", s)             # strip apostrophes
    s = re.sub(r"[^a-z0-9\s]", " ", s)      # non-alnum -> space
    s = re.sub(r"\s+", " ", s).strip()
    return s


def ensure_table(conn: sqlite3.Connection):
    # create mapping table if it doesn't exist yet
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS team_map_oddsapi (
          oddsapi_team_name TEXT PRIMARY KEY,
          team_id INTEGER,
          match_score INTEGER,
          match_method TEXT,
          matched_team_name TEXT,
          needs_review INTEGER DEFAULT 0
        )
        """
    )

    # also backfill columns in case schema changed over time
    cols = {r[1] for r in conn.execute("PRAGMA table_info(team_map_oddsapi);").fetchall()}

    def add_col(name, ddl):
        if name not in cols:
            conn.execute(f"ALTER TABLE team_map_oddsapi ADD COLUMN {name} {ddl}")

    add_col("match_score", "INTEGER")
    add_col("match_method", "TEXT")
    add_col("matched_team_name", "TEXT")
    add_col("needs_review", "INTEGER DEFAULT 0")


def is_mapped(val) -> bool:
    """
    Pandas reads NULL ints from SQLite as NaN floats.
    Treat None/NaN as unmapped, anything else as mapped.
    """
    if val is None:
        return False
    try:
        # catches NaN
        return not (isinstance(val, float) and np.isnan(val))
    except Exception:
        return True


def main():
    conn = sqlite3.connect(CFG.DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    ensure_table(conn)

    # --- Load NCAA teams ---
    teams = pd.read_sql_query("SELECT team_id, team_name FROM teams WHERE team_name IS NOT NULL;", conn)
    teams["k"] = teams["team_name"].apply(norm)
    teams = teams.dropna(subset=["team_id", "team_name", "k"])
    teams = teams[teams["k"].astype(str).str.len() > 0].copy()

    # build fuzzy-match lookup objects
    team_keys = teams["k"].tolist()
    key_to_team = dict(zip(teams["k"], zip(teams["team_id"], teams["team_name"])))

    # --- Collect OddsAPI names (only teams that actually appear in the odds feed) ---
    odds = pd.read_sql_query(
        """
        WITH names AS (
          SELECT DISTINCT home_team AS oddsapi_team_name
          FROM oddsapi_odds_snapshots
          WHERE bookmaker = ? AND market = 'spreads' AND home_team IS NOT NULL
          UNION
          SELECT DISTINCT away_team AS oddsapi_team_name
          FROM oddsapi_odds_snapshots
          WHERE bookmaker = ? AND market = 'spreads' AND away_team IS NOT NULL
        )
        SELECT oddsapi_team_name FROM names
        """,
        conn,
        params=(BOOK, BOOK),
    )

    odds = odds.dropna()
    odds["k"] = odds["oddsapi_team_name"].apply(norm)

    # make sure every seen odds name exists in mapping table
    for nm in odds["oddsapi_team_name"].unique():
        conn.execute(
            "INSERT OR IGNORE INTO team_map_oddsapi (oddsapi_team_name) VALUES (?)",
            (nm,),
        )
    conn.commit()

    # pull existing mappings so manual fixes do not get overwritten
    existing = pd.read_sql_query(
        "SELECT oddsapi_team_name, team_id FROM team_map_oddsapi;",
        conn
    )
    existing_map = dict(zip(existing["oddsapi_team_name"], existing["team_id"]))

    auto = review = low = 0

    for _, r in odds.iterrows():
        odds_name = r["oddsapi_team_name"]
        odds_key = r["k"]

        # skip already-mapped rows
        if is_mapped(existing_map.get(odds_name)):
            continue

        if not odds_key:
            low += 1
            continue

        # fuzzy match odds name -> NCAA team names
        m = process.extractOne(odds_key, team_keys, scorer=fuzz.token_set_ratio)
        if not m:
            low += 1
            continue

        best_key, score, _ = m
        team_id, team_name = key_to_team[best_key]

        if score >= AUTO_THRESHOLD:
            conn.execute(
                """
                UPDATE team_map_oddsapi
                SET team_id=?, match_score=?, match_method='fuzzy_auto',
                    matched_team_name=?, needs_review=0
                WHERE oddsapi_team_name=?
                """,
                (int(team_id), int(score), str(team_name), str(odds_name))
            )
            auto += 1

        elif score >= REVIEW_THRESHOLD:
            conn.execute(
                """
                UPDATE team_map_oddsapi
                SET team_id=NULL, match_score=?, match_method='fuzzy_review',
                    matched_team_name=?, needs_review=1
                WHERE oddsapi_team_name=?
                """,
                (int(score), str(team_name), str(odds_name))
            )
            review += 1

        else:
            conn.execute(
                """
                UPDATE team_map_oddsapi
                SET team_id=NULL, match_score=?, match_method='low_score',
                    matched_team_name=?, needs_review=0
                WHERE oddsapi_team_name=?
                """,
                (int(score), str(team_name), str(odds_name))
            )
            low += 1

    conn.commit()

    # quick final summary so I can see how much still needs attention
    totals = conn.execute(
        """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN team_id IS NOT NULL THEN 1 ELSE 0 END) AS mapped,
          SUM(CASE WHEN needs_review=1 THEN 1 ELSE 0 END) AS needs_review,
          SUM(CASE WHEN team_id IS NULL AND needs_review=0 THEN 1 ELSE 0 END) AS unmapped
        FROM team_map_oddsapi
        """
    ).fetchone()

    conn.close()

    print("Auto-mapped this run:", auto)
    print("Needs review this run:", review)
    print("Low/unmapped this run:", low)
    print("Totals -> total:", totals[0], "mapped:", totals[1], "needs_review:", totals[2], "unmapped:", totals[3])


if __name__ == "__main__":
    main()