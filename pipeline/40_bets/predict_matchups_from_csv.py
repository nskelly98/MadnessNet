# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 19:58:37 2026

@author: skell
"""

# predict_matchups_from_csv.py
#
# Scores custom matchup lists from a CSV using saved GoopNet models:
# - Reads hypothetical or bracket-style matchups from CSV
# - Maps team names to internal IDs
# - Builds the same model features used for live predictions
# - Runs all saved models + ensemble
# - Outputs projected margins / edges / suggested bets
#
# Basically this was my way to score tournament matchups before books had full lines up
#
# Usage:
#   python predict_matchups_from_csv.py --input path/to/matchups.csv
#   python predict_matchups_from_csv.py --input path/to/matchups.csv --output path/to/preds.csv
#   python predict_matchups_from_csv.py --input path/to/matchups.csv --print-all
#
# Notes:
# - Supports hypothetical matchups, not just real scheduled games
# - Neutral site can be passed in through the CSV
# - Uses the same saved models / weights as the live betting script

import sys
from pathlib import Path
import json
import sqlite3
import pandas as pd
from datetime import datetime, timezone
import argparse
import joblib

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from typing import List

# Add project root (pipeline/) to Python path
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from config import CFG


# ----------------------------
# Paths / config
# ----------------------------
MODEL_DIR = Path(getattr(CFG, "MODEL_DIR", r"C:\GoopNet\pipeline\30_models\latest"))

THRESHOLD = float(getattr(CFG, "THRESHOLD", 8.0))
MAX_UNITS_PER_BET = float(getattr(CFG, "MAX_UNITS_PER_BET", 2.0))
STAKE_BUCKETS = getattr(CFG, "STAKE_BUCKETS", ((0.0, 0.5), (2.0, 1.0), (4.0, 1.5), (6.0, 2.0)))

PRED_CAP = float(getattr(CFG, "PRED_CAP", 18.0))
EDGE_CAP = float(getattr(CFG, "EDGE_CAP", 12.0))

DEFAULT_W = {"ridge": 0.302, "random_forest": 0.114, "xgb": 0.126, "svm": 0.459}
W = getattr(CFG, "ENSEMBLE_WEIGHTS_4", DEFAULT_W)

PRIMARY_MODEL = "ens4_saved_raw"

neutral_bias_points = 0  # no extra adjustment here since this was mainly for bracket / custom matchup scoring


def iso_now_utc() -> str:
    # consistent timestamp for outputs
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def align_features(df: pd.DataFrame, feature_list: List[str]) -> pd.DataFrame:
    # add anything missing, drop extras, preserve training order
    for c in feature_list:
        if c not in df.columns:
            df[c] = 0.0
    return df[feature_list].copy()


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Path to matchup CSV")
    p.add_argument("--output", default=None, help="Optional path to save predictions CSV")
    p.add_argument("--print-all", action="store_true", help="Print all rows, not just best bets")
    return p.parse_args()


def main():
    args = parse_args()
    run_at = iso_now_utc()

    if not MODEL_DIR.exists():
        raise RuntimeError(f"MODEL_DIR not found: {MODEL_DIR}")

    conn = sqlite3.connect(CFG.DB_PATH)

    # ----------------------------
    # Read matchup list
    # ----------------------------
    games = pd.read_csv(args.input)

    required_cols = {"home_team", "away_team"}
    missing = required_cols - set(games.columns)
    if missing:
        conn.close()
        raise RuntimeError(f"Missing required columns in input CSV: {sorted(missing)}")

    # defaults so the CSV can stay simple
    if "spread_home" not in games.columns:
        games["spread_home"] = 0.0

    if "neutral_site" not in games.columns:
        games["neutral_site"] = 1

    games["spread_home"] = pd.to_numeric(games["spread_home"], errors="coerce").fillna(0.0)
    games["neutral_site"] = pd.to_numeric(games["neutral_site"], errors="coerce").fillna(0).astype(int)

    # lightweight IDs just so outputs are easier to track
    games = games.copy()
    games["event_id"] = [f"custom_{i+1}" for i in range(len(games))]
    games["bookmaker"] = "CUSTOM"
    games["game_date"] = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    games["commence_time_utc"] = None

    # ----------------------------
    # Team mapping
    # ----------------------------
    team_map = pd.read_sql_query(
        """
        SELECT team_id, oddsapi_team_name
        FROM team_map_oddsapi
        WHERE oddsapi_team_name IS NOT NULL
        """,
        conn,
    )
    odds_to_team_id = dict(zip(team_map["oddsapi_team_name"], team_map["team_id"]))

    games["home_team_id"] = games["home_team"].map(odds_to_team_id)
    games["away_team_id"] = games["away_team"].map(odds_to_team_id)

    unmapped = games[games["home_team_id"].isna() | games["away_team_id"].isna()].copy()
    if not unmapped.empty:
        print("\nUNMAPPED TEAMS — THESE ROWS WILL BE SKIPPED")
        print(unmapped[["home_team", "away_team"]].to_string(index=False))
        print()

    games = games.drop(unmapped.index).copy()

    if games.empty:
        conn.close()
        raise RuntimeError("No usable games remain after team mapping.")

    games["home_team_id"] = games["home_team_id"].astype(int)
    games["away_team_id"] = games["away_team_id"].astype(int)

    # ----------------------------
    # MIN_GAMES gating
    # ----------------------------
    gp = pd.read_sql_query(
        """
        SELECT team_id, COUNT(*) AS games_played
        FROM team_rolling_stats
        GROUP BY team_id
        """,
        conn,
    )
    gp_map = dict(zip(gp["team_id"], gp["games_played"]))

    # ----------------------------
    # Latest rolling stats row per team
    # ----------------------------
    trs = pd.read_sql_query(
        """
        WITH latest AS (
          SELECT team_id, MAX(game_id) AS max_game_id
          FROM team_rolling_stats
          GROUP BY team_id
        )
        SELECT r.*
        FROM team_rolling_stats r
        JOIN latest l
          ON l.team_id = r.team_id
         AND l.max_game_id = r.game_id
        """,
        conn,
    )

    if trs.empty:
        conn.close()
        raise RuntimeError("team_rolling_stats is empty. Run compute_rolling_stats.py first.")

    trs = trs.set_index("team_id")

    def feat(team_id: int, col: str, default=0.0) -> float:
        # safe lookup for latest rolling stats
        try:
            v = trs.at[team_id, col]
            return default if pd.isna(v) else float(v)
        except Exception:
            return default

    # ----------------------------
    # Build feature rows
    # ----------------------------
    rows = []
    skipped_min_games = 0

    for _, r in games.iterrows():
        hid = int(r["home_team_id"])
        aid = int(r["away_team_id"])
        neutral = int(r["neutral_site"])

        if gp_map.get(hid, 0) < CFG.MIN_GAMES or gp_map.get(aid, 0) < CFG.MIN_GAMES:
            skipped_min_games += 1
            continue

        # same feature setup as live script
        # for neutral games, turn home-court off
        home_court_adv = 0.0 if neutral == 1 else float(getattr(CFG, "HOME_COURT_ADV", 0.0))

        rows.append({
            "run_at_utc": run_at,
            "game_date": r["game_date"],
            "event_id": r["event_id"],
            "home_team": r["home_team"],
            "away_team": r["away_team"],
            "bookmaker": r["bookmaker"],
            "spread_home_req": float(r["spread_home"]),
            "commence_time_utc": r["commence_time_utc"],
            "neutral_site": neutral,

            # if this feature existed in training, align_features keeps it
            # if not, it gets dropped cleanly
            "home_court_adv": home_court_adv,

            "delta_roll5_adj_em": feat(hid, "roll5_adj_em") - feat(aid, "roll5_adj_em"),
            "delta_roll10_adj_em": feat(hid, "roll10_adj_em") - feat(aid, "roll10_adj_em"),
            "delta_ewm_adj_em": feat(hid, "ewm_adj_em") - feat(aid, "ewm_adj_em"),
            "delta_roll5_off_eff": feat(hid, "roll5_off_eff") - feat(aid, "roll5_off_eff"),
            "delta_roll10_def_eff": feat(hid, "roll10_def_eff") - feat(aid, "roll10_def_eff"),
            "delta_trend_adj_em": feat(hid, "trend_adj_em") - feat(aid, "trend_adj_em"),
            "delta_adj_em_like": 0.0,
            "delta_sos_em_like": 0.0,
            "delta_luck_like": 0.0,

            "home_spread": float(r["spread_home"]),
            "away_spread": float(-float(r["spread_home"])),
        })

    if not rows:
        conn.close()
        raise RuntimeError(
            f"No games survived MIN_GAMES={CFG.MIN_GAMES}. "
            f"Skipped by MIN_GAMES={skipped_min_games}."
        )

    live = pd.DataFrame(rows)

    # ----------------------------
    # Load models + features
    # ----------------------------
    feature_list = json.loads((MODEL_DIR / "feature_list.json").read_text())

    ridge = joblib.load(MODEL_DIR / "ridge.joblib")
    svm = joblib.load(MODEL_DIR / "svr.joblib")
    rf = joblib.load(MODEL_DIR / "rf.joblib")
    xgb = joblib.load(MODEL_DIR / "xgb.joblib")

    X_live = align_features(live.copy(), feature_list)

    # ----------------------------
    # Predict
    # ----------------------------
    live["pred_ridge"] = ridge.predict(X_live)
    live["pred_svm"] = svm.predict(X_live)
    live["pred_rf"] = rf.predict(X_live)
    live["pred_xgb"] = xgb.predict(X_live.to_numpy(dtype="float32"))

    live["edge_ridge"] = live["pred_ridge"] + live["spread_home_req"]
    live["edge_svm"] = live["pred_svm"] + live["spread_home_req"]
    live["edge_rf"] = live["pred_rf"] + live["spread_home_req"]
    live["edge_xgb"] = live["pred_xgb"] + live["spread_home_req"]

    live["pred_ens_raw"] = (
        float(W["ridge"]) * live["pred_ridge"]
        + float(W["svm"]) * live["pred_svm"]
        + float(W["random_forest"]) * live["pred_rf"]
        + float(W["xgb"]) * live["pred_xgb"]
    )

    live["pred_primary"] = live["pred_ens_raw"] - neutral_bias_points
    live["edge_primary"] = live["pred_primary"] + live["spread_home_req"]

    # only let sane predictions qualify as bets
    sane = (live["pred_primary"].abs() <= PRED_CAP) & (live["edge_primary"].abs() <= EDGE_CAP)

    live["bet_side"] = "NO_BET"
    live.loc[sane & (live["edge_primary"] >= THRESHOLD), "bet_side"] = "HOME"
    live.loc[sane & (live["edge_primary"] <= -THRESHOLD), "bet_side"] = "AWAY"

    live["qualifies_threshold"] = (live["bet_side"] != "NO_BET").astype(int)

    # same stake sizing as live script
    abs_edge = live["edge_primary"].abs()
    live["stake_units"] = 0.0
    for k, u in STAKE_BUCKETS:
        live.loc[sane & (abs_edge >= (THRESHOLD + float(k))), "stake_units"] = float(u)

    live["stake_units"] = live["stake_units"].clip(upper=float(MAX_UNITS_PER_BET))
    live.loc[live["bet_side"] == "NO_BET", "stake_units"] = 0.0
    live["qualifies_bet"] = (live["stake_units"] > 0).astype(int)
    live.loc[live["qualifies_bet"] == 0, "bet_side"] = "NO_BET"

    # ----------------------------
    # Output table
    # ----------------------------
    out = live[[
        "home_team",
        "away_team",
        "neutral_site",
        "spread_home_req",
        "pred_ridge",
        "pred_svm",
        "pred_rf",
        "pred_xgb",
        "pred_primary",
        "edge_primary",
        "bet_side",
        "stake_units",
        "qualifies_threshold",
        "qualifies_bet",
    ]].copy()

    out = out.rename(columns={
        "spread_home_req": "home_spread",
        "pred_primary": "pred_margin_home",
        "edge_primary": "edge_home",
    })

    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(args.output, index=False)
        print(f"Saved predictions to: {args.output}")

    print("===================================")
    print("CUSTOM MATCHUP PREDICTIONS")
    print("Run:", run_at)
    print("Primary:", PRIMARY_MODEL)
    print("Weights:", W)
    print("Threshold:", THRESHOLD)
    print("MIN_GAMES:", CFG.MIN_GAMES)
    print("Caps: |pred| <=", PRED_CAP, " |edge| <=", EDGE_CAP)
    print("Games scored:", len(out))
    print("===================================")

    if args.print_all:
        print(out.to_string(index=False))
    else:
        bets_only = out[out["qualifies_bet"] == 1].copy()
        if bets_only.empty:
            print("No qualifying bets in input list.")
            print("\nAll scored games:")
            print(out[["away_team", "home_team", "home_spread", "pred_margin_home", "edge_home", "bet_side"]].to_string(index=False))
        else:
            bets_only["abs_edge"] = bets_only["edge_home"].abs()
            bets_only = bets_only.sort_values("abs_edge", ascending=False)
            for _, r in bets_only.iterrows():
                print(
                    f'{r["away_team"]} @ {r["home_team"]} | {r["bet_side"]} | '
                    f'line(H)={float(r["home_spread"]):+.1f} | '
                    f'pred(H margin)={float(r["pred_margin_home"]):+.1f} | '
                    f'edge={float(r["edge_home"]):+.1f} | '
                    f'stake={float(r["stake_units"]):.1f}u | '
                    f'neutral={int(r["neutral_site"])}'
                )

    conn.close()


if __name__ == "__main__":
    main()