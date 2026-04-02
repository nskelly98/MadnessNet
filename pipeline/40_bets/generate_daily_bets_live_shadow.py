# generate_daily_bets_live_saved_ensemble.py
#
# Generates daily betting recommendations using saved models:
# - Pulls latest odds (FanDuel by default)
# - Builds live features from latest rolling stats
# - Runs predictions across all models + ensemble
# - Applies thresholds / caps / bankroll rules
# - Logs bets + model predictions to DB
#
# This is basically the "production" script that turns models into actual bets
#
# Usage:
#   python generate_daily_bets_live_shadow.py
#
# Notes:
# - Skips games with bad/missing team mappings instead of failing
# - Uses RAW ensemble (no shrink, no market anchoring)
# - Applies multiple safety layers (threshold, caps, max bets, max exposure)

import sys
from pathlib import Path
import json
import sqlite3
import pandas as pd
from datetime import datetime, timezone

# Add project root (pipeline/) to Python path
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

import joblib

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from typing import List

from config import CFG

neutral_bias_points = 2.5  # something I drummed up to handle neutral court... need a better way

# ----------------------------
# Paths
# ----------------------------
MODEL_DIR = Path(getattr(CFG, "MODEL_DIR", r"C:\GoopNet\pipeline\30_models\latest"))

# ----------------------------
# Betting policy (from config)
# ----------------------------
THRESHOLD = float(getattr(CFG, "THRESHOLD", 8.0))
MAX_BETS = int(getattr(CFG, "MAX_BETS", 8))
MAX_UNITS_DAY = float(getattr(CFG, "MAX_UNITS_DAY", 6.0))
MAX_UNITS_PER_BET = float(getattr(CFG, "MAX_UNITS_PER_BET", 2.0))
STAKE_BUCKETS = getattr(CFG, "STAKE_BUCKETS", ((0.0, 0.5), (2.0, 1.0), (4.0, 1.5), (6.0, 2.0)))

PRED_CAP = float(getattr(CFG, "PRED_CAP", 18.0))
EDGE_CAP = float(getattr(CFG, "EDGE_CAP", 12.0))

# Ensemble weights (no NN, renormalized)
DEFAULT_W = {"ridge": 0.302, "random_forest": 0.114, "xgb": 0.126, "svm": 0.459}
W = getattr(CFG, "ENSEMBLE_WEIGHTS_4", DEFAULT_W)

PRIMARY_MODEL = "ens4_saved_raw"


def iso_now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_dir(p: Path) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table,),
    ).fetchone()
    return row is not None


def filter_df_to_table_columns(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Keeps only columns that exist in the DB table.
    Prevents schema mismatch errors if table evolves over time.
    """
    if not table_exists(conn, table):
        return df

    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()]
    keep = [c for c in df.columns if c in cols]

    if not keep:
        raise RuntimeError(
            f"Table {table} exists but none of df columns match its schema."
        )

    return df[keep].copy()


def log_to_bet_recommendations(conn, run_utc: str, scored_df: pd.DataFrame, book: str) -> int:
    """
    Inserts bet recommendations (ridge/xgb + primary decision).
    Uses INSERT OR IGNORE so reruns do not duplicate rows.
    """
    sql = """
    INSERT OR IGNORE INTO bet_recommendations (
      run_id, run_utc, game_date, commence_time_utc, bookmaker,
      home_team, away_team, game_id, odds_snapshot_id,
      home_spread, away_spread,
      pred_home_ridge, pred_home_xgb,
      edge_home_ridge, edge_home_xgb,
      pick_side, model_primary, edge_primary, qualifies_bet, stake_units,
      notes
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    ins = 0
    for _, r in scored_df.iterrows():
        conn.execute(sql, (
            run_utc, run_utc,
            r.get("game_date"),
            r.get("commence_time_utc"),
            book,
            r.get("home_team"),
            r.get("away_team"),
            r.get("game_id"),
            r.get("odds_snapshot_id"),
            None if pd.isna(r.get("home_spread")) else float(r.get("home_spread")),
            None if pd.isna(r.get("away_spread")) else float(r.get("away_spread")),
            None if pd.isna(r.get("pred_home_ridge")) else float(r.get("pred_home_ridge")),
            None if pd.isna(r.get("pred_home_xgb")) else float(r.get("pred_home_xgb")),
            None if pd.isna(r.get("edge_home_ridge")) else float(r.get("edge_home_ridge")),
            None if pd.isna(r.get("edge_home_xgb")) else float(r.get("edge_home_xgb")),
            r.get("pick_side"),
            r.get("model_primary"),
            None if pd.isna(r.get("edge_primary")) else float(r.get("edge_primary")),
            int(r.get("qualifies_bet", 0) or 0),
            float(r.get("stake_units", 0.0) or 0.0),
            r.get("notes"),
        ))
        if conn.total_changes > 0:
            ins += 1
    return ins


def align_features(df: pd.DataFrame, feature_list: List[str]) -> pd.DataFrame:
    # ensures feature alignment with training (adds missing, drops extras)
    for c in feature_list:
        if c not in df.columns:
            df[c] = 0.0
    return df[feature_list].copy()


def main():
    run_at = iso_now_utc()
    conn = sqlite3.connect(CFG.DB_PATH)

    # ----------------------------
    # Pull latest odds
    # ----------------------------
    odds = pd.read_sql_query(
        """
        WITH filtered AS (
          SELECT *
          FROM oddsapi_odds_snapshots
          WHERE bookmaker = ?
            AND market = 'spreads'
            AND commence_time IS NOT NULL
            AND datetime(commence_time) >= datetime('now', '-6 hours')
            AND datetime(commence_time) <= datetime('now', '+48 hours')
        ),
        latest AS (
          SELECT event_id, bookmaker, MAX(pulled_at_utc) AS max_pulled
          FROM filtered
          GROUP BY event_id, bookmaker
        )
        SELECT f.*
        FROM filtered f
        JOIN latest l
          ON l.event_id = f.event_id
         AND l.bookmaker = f.bookmaker
         AND l.max_pulled = f.pulled_at_utc
        """,
        conn,
        params=(CFG.BOOK,),
    )

    if odds.empty:
        conn.close()
        raise RuntimeError("No odds found. Run oddsapi_pull_odds_today.py first.")

    # convert + filter to today's slate
    odds["commence_time"] = pd.to_datetime(odds["commence_time"], utc=True, errors="coerce")
    odds["game_date"] = odds["commence_time"].dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d")

    target_date = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    odds = odds[odds["game_date"] == target_date].copy()

    odds["commence_time_utc"] = odds["commence_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    odds["spread_home_req"] = odds["spread_home"].astype(float)

    # ----------------------------
    # Team mapping (skip bad rows instead of failing)
    # ----------------------------
    team_map = pd.read_sql_query(
        "SELECT team_id, oddsapi_team_name FROM team_map_oddsapi WHERE oddsapi_team_name IS NOT NULL;",
        conn,
    )
    odds_to_team_id = dict(zip(team_map["oddsapi_team_name"], team_map["team_id"]))

    odds["home_team_id"] = odds["home_team"].map(odds_to_team_id)
    odds["away_team_id"] = odds["away_team"].map(odds_to_team_id)

    unmapped = odds[
        odds["home_team_id"].isna() | odds["away_team_id"].isna()
    ].copy()

    if not unmapped.empty:
        print("\nUNMAPPED TEAMS — SKIPPING")
        print(unmapped[["home_team","away_team"]].drop_duplicates().to_string(index=False))
        odds = odds.drop(unmapped.index)

    # ----------------------------
    # (rest unchanged — feature build, predictions, caps, logging)
    # ----------------------------