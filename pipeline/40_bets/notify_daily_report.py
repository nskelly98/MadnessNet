# notify_daily_report.py
#
# Builds and sends the daily GoopNet report to Discord:
# - Pulls latest qualifying picks from the most recent run
# - Pulls yesterday's graded results
# - Summarizes overall record / units / ROI
# - Posts the report to Discord in chunks if needed
#
# Basically this is the daily status update script so I can see picks + performance in one place
#
# Usage:
#   python notify_daily_report.py
#
# Notes:
# - Uses latest run_utc for the configured bookmaker
# - Matches in per-model predictions when event_id is available
# - Splits long messages to stay under Discord's message limit

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))  # allow config import when run directly

import sqlite3
import pandas as pd
import requests
from datetime import datetime, timedelta

from config import CFG

MAX_LEN = 1900  # Discord hard limit is ~2000, so leave a little buffer


def yesterday_local_date_str():
    # used for the "last night results" section
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def build_report(conn: sqlite3.Connection):
    yday = yesterday_local_date_str()

    # ----------------------------
    # 1) Latest run for this bookmaker
    # ----------------------------
    latest = pd.read_sql_query(
        """
        SELECT MAX(run_utc) AS run_utc
        FROM bet_recommendations
        WHERE bookmaker = ?
        """,
        conn,
        params=(CFG.BOOK,),
    )
    run_utc = latest.iloc[0]["run_utc"]

    if run_utc is None:
        return "No runs found in bet_recommendations yet."

    # ----------------------------
    # 2) Picks from latest run
    # ----------------------------
    # Adds event_id by matching back to odds snapshots and taking the latest
    # pull at or before the run time
    picks = pd.read_sql_query(
        """
        WITH picks_base AS (
          SELECT
            br.game_date,
            br.commence_time_utc,
            br.home_team, br.away_team,
            br.home_spread,
            br.bookmaker,
            br.game_id,
            br.odds_snapshot_id,
            br.model_primary,
            br.edge_primary,
            br.pick_side,
            br.qualifies_bet,
            COALESCE(br.stake_units, 1.0) AS stake_units,
            br.pred_home_ridge,
            br.pred_home_xgb,
            br.edge_home_ridge,
            br.edge_home_xgb
          FROM bet_recommendations br
          WHERE br.bookmaker = ?
            AND br.run_utc = ?
            AND br.qualifies_bet = 1
        ),
        mapped AS (
          SELECT
            p.*,
            (
              SELECT os.event_id
              FROM oddsapi_odds_snapshots os
              WHERE os.bookmaker = p.bookmaker
                AND os.market = 'spreads'
                AND os.home_team = p.home_team
                AND os.away_team = p.away_team
                AND datetime(os.commence_time) = datetime(p.commence_time_utc)
                AND datetime(os.pulled_at_utc) <= datetime(?)
              ORDER BY datetime(os.pulled_at_utc) DESC
              LIMIT 1
            ) AS event_id
          FROM picks_base p
        )
        SELECT *
        FROM mapped
        ORDER BY game_date, ABS(edge_primary) DESC
        """,
        conn,
        params=(CFG.BOOK, run_utc, run_utc),
    )

    # Pull per-model predictions from the same run
    preds = pd.read_sql_query(
        """
        SELECT
          run_at_utc,
          event_id,
          bookmaker,
          model_name,
          pred_margin
        FROM daily_model_predictions
        WHERE bookmaker = ?
          AND run_at_utc = ?
          AND model_name IN ('primary','ridge','xgb','svm','rf','ens_raw')
        """,
        conn,
        params=(CFG.BOOK, run_utc),
    )

    # Pivot predictions wide so they are easier to merge/display
    if not picks.empty and not preds.empty:
        pred_wide = (
            preds.pivot_table(
                index=["run_at_utc", "event_id", "bookmaker"],
                columns="model_name",
                values="pred_margin",
                aggfunc="last",
            )
            .reset_index()
            .rename(
                columns={
                    "primary": "pred_primary",
                    "ridge": "pred_ridge",
                    "xgb": "pred_xgb",
                    "svm": "pred_svm",
                    "rf": "pred_rf",
                    "ens_raw": "pred_ens_raw",
                }
            )
        )

        # merge on event_id when available, but keep rows even if event_id is null
        picks = picks.merge(
            pred_wide,
            how="left",
            on=["event_id", "bookmaker"],
        )

    # make sure pred_primary exists even if merge failed / no event_id
    if "pred_primary" not in picks.columns:
        picks["pred_primary"] = pd.NA

    # fallback to stored xgb/ridge pred if primary is missing
    if not picks.empty:
        picks["pred_primary"] = picks["pred_primary"].where(
            ~picks["pred_primary"].isna(),
            picks["pred_home_xgb"].where(~picks["pred_home_xgb"].isna(), picks["pred_home_ridge"]),
        )

    # ----------------------------
    # 3) Last night results
    # ----------------------------
    last = pd.read_sql_query(
        """
        SELECT
          game_date, home_team, away_team,
          home_spread, pick_side,
          final_home_score, final_away_score,
          ats_result,
          COALESCE(stake_units, 1.0) AS stake_units,
          COALESCE(profit_units, 0.0) AS profit_units
        FROM bet_recommendations
        WHERE game_date = ?
          AND qualifies_bet = 1
          AND graded_at_utc IS NOT NULL
        ORDER BY rec_id DESC
        """,
        conn,
        params=(yday,),
    )

    # ----------------------------
    # 4) Overall performance
    # ----------------------------
    overall = pd.read_sql_query(
        """
        SELECT
          COUNT(*) AS bets,
          SUM(CASE WHEN ats_result='W' THEN 1 ELSE 0 END) AS wins,
          SUM(CASE WHEN ats_result='L' THEN 1 ELSE 0 END) AS losses,
          SUM(CASE WHEN ats_result='P' THEN 1 ELSE 0 END) AS pushes,
          SUM(COALESCE(stake_units, 1.0)) AS staked_units,
          SUM(COALESCE(profit_units, 0.0)) AS units
        FROM bet_recommendations
        WHERE qualifies_bet=1 AND graded_at_utc IS NOT NULL
        """,
        conn,
    ).iloc[0].to_dict()

    bets = int(overall["bets"] or 0)
    wins = int(overall["wins"] or 0)
    losses = int(overall["losses"] or 0)
    pushes = int(overall["pushes"] or 0)
    units = float(overall["units"] or 0.0)
    staked_units = float(overall["staked_units"] or 0.0)
    roi = (units / staked_units) if staked_units > 0 else 0.0

    lines = []
    lines.append("**GoopNet Daily Report — Latest Run**")
    lines.append(f"Book: **{CFG.BOOK}** | run_utc: `{run_utc}`")
    lines.append("")

    # ----------------------------
    # PICKS (latest run)
    # ----------------------------
    lines.append("**PICKS (from latest run)**")
    if picks.empty:
        lines.append("_No qualifying bets in the latest run._")
    else:
        picks = picks.copy()
        picks["home_spread"] = pd.to_numeric(picks["home_spread"], errors="coerce").fillna(0.0)

        # primary display prediction = ensemble primary margin
        picks["pred_home_primary"] = pd.to_numeric(picks["pred_primary"], errors="coerce").fillna(0.0)
        picks["fair_line_home_primary"] = -picks["pred_home_primary"]

        for gd, gdf in picks.groupby("game_date", sort=True):
            lines.append(f"\n**{gd}**")
            gdf = gdf.sort_values("edge_primary", key=lambda s: s.abs(), ascending=False)

            for _, r in gdf.iterrows():
                away = r["away_team"]
                home = r["home_team"]
                side = r["pick_side"]
                line_home = float(r["home_spread"])
                pred = float(r["pred_home_primary"])
                edge = float(r["edge_primary"])
                stake = float(r["stake_units"] or 1.0)

                warn = ""
                if pd.isna(r.get("event_id")):
                    warn = " ⚠️no_event_id"

                lines.append(
                    f"- {away} @ {home} | **{side}** | "
                    f"**stake `{stake:.1f}u`** | "
                    f"line(H) `{line_home:+.1f}` | "
                    f"model_margin `{pred:+.1f}` | "
                    f"edge `{edge:+.1f}`{warn}"
                )

        total_stake = float(picks["stake_units"].fillna(1.0).sum())
        lines.append(f"\nTotal stake (latest run): **{total_stake:.1f}u**")

    lines.append("\n---\n")

    # ----------------------------
    # LAST NIGHT RESULTS
    # ----------------------------
    lines.append(f"**LAST NIGHT RESULTS ({yday})**")
    if last.empty:
        lines.append("_No graded bets found for yesterday yet._")
    else:
        w = int((last["ats_result"] == "W").sum())
        l = int((last["ats_result"] == "L").sum())
        p = int((last["ats_result"] == "P").sum())
        u = float(last["profit_units"].fillna(0).sum())
        st = float(last["stake_units"].fillna(1.0).sum())
        roi_y = (u / st) if st > 0 else 0.0

        lines.append(
            f"Summary: **{w}-{l}-{p}** | Stake: **{st:.1f}u** | Units: **{u:+.2f}u** | ROI: **{roi_y*100:.1f}%**"
        )

        for _, r in last.iterrows():
            away = r["away_team"]
            home = r["home_team"]
            side = r["pick_side"]
            line_home = float(r["home_spread"])
            stake = float(r["stake_units"] or 1.0)
            score = f'{int(r["final_away_score"])}-{int(r["final_home_score"])}'
            res = r["ats_result"]
            pu = float(r["profit_units"] or 0.0)

            lines.append(
                f"- {away} @ {home} | {side} | line(H) `{line_home:+.1f}` | "
                f"stake `{stake:.1f}u` | score(A-H) `{score}` | **{res}** `{pu:+.2f}u`"
            )

    lines.append("")
    lines.append("**OVERALL (graded bets)**")
    lines.append(
        f"Record: **{wins}-{losses}-{pushes}** | Bets: **{bets}** | "
        f"Stake: **{staked_units:.1f}u** | Units: **{units:+.2f}u** | ROI: **{roi*100:.2f}%**"
    )

    return "\n".join(lines)


def post_discord(content: str):
    if not getattr(CFG, "DISCORD_ENABLED", False):
        print("Discord disabled in config.")
        return

    url = getattr(CFG, "DISCORD_WEBHOOK_URL", None)
    if not url:
        raise RuntimeError("DISCORD_WEBHOOK_URL is not set in config.py")

    # split into chunks so long reports do not fail
    chunks = []
    s = content
    while len(s) > MAX_LEN:
        cut = s.rfind("\n", 0, MAX_LEN)
        if cut == -1:
            cut = MAX_LEN
        chunks.append(s[:cut])
        s = s[cut:].lstrip("\n")
    chunks.append(s)

    for i, chunk in enumerate(chunks, start=1):
        payload = {"content": chunk}
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        print(f"Posted Discord message chunk {i}/{len(chunks)}")


def main():
    conn = sqlite3.connect(CFG.DB_PATH)
    report = build_report(conn)
    conn.close()

    post_discord(report)
    print("Discord report sent.")


if __name__ == "__main__":
    main()