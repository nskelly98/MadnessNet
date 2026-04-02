# -*- coding: utf-8 -*-
"""
Created on Mon Jan 26 14:04:21 2026

@author: skell
"""
import sys
from pathlib import Path

# Add project root (pipeline/) to Python path
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))


import sqlite3
import pandas as pd
import numpy as np

from config import CFG

def compute_slope(y: np.ndarray) -> float:
    n = len(y)
    if n < 2:
        return np.nan
    x = np.arange(n, dtype=float)
    x_mean = x.mean()
    y_mean = y.mean()
    denom = ((x - x_mean) ** 2).sum()
    if denom == 0:
        return np.nan
    return float(((x - x_mean) * (y - y_mean)).sum() / denom)

def main():
    conn = sqlite3.connect(CFG.DB_PATH)

    df = pd.read_sql_query(
        """
        SELECT
            tgs.team_id,
            tgs.game_id,
            g.game_date,
            tgs.off_eff,
            tgs.def_eff
        FROM team_game_stats tgs
        JOIN games g ON g.game_id = tgs.game_id
        ORDER BY tgs.team_id, g.game_date, tgs.game_id
        """,
        conn,
    )

    if df.empty:
        conn.close()
        raise RuntimeError("No rows in team_game_stats/games join. Ingest games and team_game_stats first.")

    df["game_date"] = pd.to_datetime(df["game_date"])
    df["adj_em_proxy"] = df["off_eff"] - df["def_eff"]

    rows = []
    for team_id, gdf in df.groupby("team_id", sort=False):
        gdf = gdf.sort_values(["game_date", "game_id"]).reset_index(drop=True)

        roll5_adj = gdf["adj_em_proxy"].shift(1).rolling(5, min_periods=1).mean()
        roll10_adj = gdf["adj_em_proxy"].shift(1).rolling(10, min_periods=1).mean()

        roll5_off = gdf["off_eff"].shift(1).rolling(5, min_periods=1).mean()
        roll10_def = gdf["def_eff"].shift(1).rolling(10, min_periods=1).mean()

        ewm = gdf["adj_em_proxy"].shift(1).ewm(alpha=CFG.ROLL_ALPHA, adjust=False).mean()

        prior_adj = gdf["adj_em_proxy"].shift(1).to_numpy()
        trend = []
        for i in range(len(prior_adj)):
            window = prior_adj[max(0, i - 10):i]
            window = window[~np.isnan(window)]
            trend.append(compute_slope(window) if len(window) >= 2 else np.nan)
        trend = pd.Series(trend)

        out = pd.DataFrame({
            "team_id": team_id,
            "game_id": gdf["game_id"],
            "roll5_adj_em": roll5_adj,
            "roll10_adj_em": roll10_adj,
            "ewm_adj_em": ewm,
            "roll5_off_eff": roll5_off,
            "roll10_def_eff": roll10_def,
            "trend_adj_em": trend,
        })
        rows.append(out)

    out_all = pd.concat(rows, ignore_index=True)

    conn.execute("DELETE FROM team_rolling_stats;")
    conn.commit()
    out_all.to_sql("team_rolling_stats", conn, if_exists="append", index=False)

    conn.close()
    print("Rolling stats computed and written to team_rolling_stats.")
    print("Rows:", len(out_all))

if __name__ == "__main__":
    main()
