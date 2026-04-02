# -*- coding: utf-8 -*-
"""
Created on Mon Jan 26 14:05:19 2026

@author: skell
"""

# pipeline/20_features/build_model_games_enriched.py
#
# Builds model-ready dataset at the game level:
# - Joins final game results with rolling team stats
# - Creates delta features (home - away)
# - Outputs model_games_enriched table used for training
#
# Basically takes raw + rolling stats and turns them into something the model can use
#
# Usage:
#   python build_model_games_enriched.py
#
# Notes:
# - Assumes rolling stats are already shifted (no data leakage)
# - Drops early-season games where rolling stats aren't populated yet
# - Rebuilds the table each run (not incremental)

import sys
from pathlib import Path

# Add project root (pipeline/) to Python path
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))  # so imports work when running scripts directly


import sqlite3
import pandas as pd
import numpy as np

from config import CFG

def main():
    conn = sqlite3.connect(CFG.DB_PATH)

    # Pull completed games (need final scores for labels)
    games = pd.read_sql_query(
        """
        SELECT
          game_id,
          game_date,
          home_team_id,
          away_team_id,
          home_score,
          away_score
        FROM games
        WHERE home_score IS NOT NULL AND away_score IS NOT NULL
        """,
        conn,
    )

    if games.empty:
        conn.close()
        raise RuntimeError("No completed games in games table (missing scores).")

    games["game_date"] = pd.to_datetime(games["game_date"])
    games["home_margin"] = games["home_score"] - games["away_score"]  # target variable

    # Rolling stats per team per game (already shifted -> no leakage)
    trs = pd.read_sql_query(
        """
        SELECT
          team_id,
          game_id,
          roll5_adj_em,
          roll10_adj_em,
          ewm_adj_em,
          roll5_off_eff,
          roll10_def_eff,
          trend_adj_em
        FROM team_rolling_stats
        """,
        conn,
    )

    if trs.empty:
        conn.close()
        raise RuntimeError("team_rolling_stats is empty. Run compute_rolling_stats.py first.")

    # Split into home/away versions so we can join cleanly
    home = trs.rename(columns={
        "team_id": "home_team_id",
        "roll5_adj_em": "home_roll5_adj_em",
        "roll10_adj_em": "home_roll10_adj_em",
        "ewm_adj_em": "home_ewm_adj_em",
        "roll5_off_eff": "home_roll5_off_eff",
        "roll10_def_eff": "home_roll10_def_eff",
        "trend_adj_em": "home_trend_adj_em",
    })
    away = trs.rename(columns={
        "team_id": "away_team_id",
        "roll5_adj_em": "away_roll5_adj_em",
        "roll10_adj_em": "away_roll10_adj_em",
        "ewm_adj_em": "away_ewm_adj_em",
        "roll5_off_eff": "away_roll5_off_eff",
        "roll10_def_eff": "away_roll10_def_eff",
        "trend_adj_em": "away_trend_adj_em",
    })

    # Join everything together at the game level
    df = games.merge(home, on=["game_id", "home_team_id"], how="left").merge(
        away, on=["game_id", "away_team_id"], how="left"
    )

    # Feature engineering (all deltas are home - away)
    df["home_court_adv"] = CFG.HOME_COURT_ADV
    df["delta_roll5_adj_em"] = df["home_roll5_adj_em"] - df["away_roll5_adj_em"]
    df["delta_roll10_adj_em"] = df["home_roll10_adj_em"] - df["away_roll10_adj_em"]
    df["delta_ewm_adj_em"] = df["home_ewm_adj_em"] - df["away_ewm_adj_em"]
    df["delta_roll5_off_eff"] = df["home_roll5_off_eff"] - df["away_roll5_off_eff"]
    df["delta_roll10_def_eff"] = df["home_roll10_def_eff"] - df["away_roll10_def_eff"]
    df["delta_trend_adj_em"] = df["home_trend_adj_em"] - df["away_trend_adj_em"]

    # placeholders for future features (kenpom-ish / sos / luck etc)
    # leaving these in so I don't have to rework the pipeline later
    if "delta_adj_em_like" not in df.columns:
        df["delta_adj_em_like"] = 0.0
    if "delta_sos_em_like" not in df.columns:
        df["delta_sos_em_like"] = 0.0
    if "delta_luck_like" not in df.columns:
        df["delta_luck_like"] = 0.0

    # Only keep what the model actually needs
    keep = [
        "game_id", "game_date", "home_team_id", "away_team_id",
        "home_score", "away_score", "home_margin",
        *CFG.FEATURES
    ]
    out = df[keep].copy()

    # Drop early-season games where rolling stats aren't populated yet
    out = out.dropna(subset=[
        "delta_roll5_adj_em","delta_roll10_adj_em","delta_ewm_adj_em",
        "delta_roll5_off_eff","delta_roll10_def_eff","delta_trend_adj_em"
    ])

    # overwrite each run (this is basically a rebuild step)
    out.to_sql("model_games_enriched", conn, if_exists="replace", index=False)
    conn.close()

    print("Built model_games_enriched.")
    print("Rows:", len(out))
    print("Date range:", out["game_date"].min(), "to", out["game_date"].max())


if __name__ == "__main__":
    main()