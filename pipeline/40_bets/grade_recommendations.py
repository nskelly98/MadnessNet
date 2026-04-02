# grade_recommendations.py
#
# Grades placed bets against actual game results:
# - Links bet_recommendations to games table (via game_id or fallback)
# - Computes ATS result (W/L/P)
# - Calculates profit in units (-110 assumed)
# - Updates DB with final scores + results
#
# Basically closes the loop so I can track performance over time
#
# Usage:
#   python grade_recommendations.py
#
# Notes:
# - Only grades the latest recommendation per matchup (avoids duplicates)
# - Uses best-effort matching if game_id is missing
# - Skips games that are not finished yet

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))  # allow config import

import sqlite3
from datetime import datetime, timezone

from config import CFG


def utc_now_iso() -> str:
    # timestamp for grading
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def table_cols(conn: sqlite3.Connection, table: str):
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()]


def grade_ats(home_score, away_score, home_spread, pick_side):
    """
    Grades ATS result for a single bet.

    home_spread: line for HOME team (ex: -4.5)
    pick_side: "HOME" or "AWAY"

    Returns:
      ("W", +0.9091), ("L", -1.0), ("P", 0.0)
    """
    if home_score is None or away_score is None or home_spread is None or pick_side is None:
        return ("N/A", None)

    margin = float(home_score) - float(away_score)  # home margin
    hs = float(home_spread)

    if str(pick_side).upper() == "HOME":
        cover = margin + hs
    else:
        cover = (-margin) + (-hs)

    if cover > 0:
        return ("W", 0.9091)
    if cover < 0:
        return ("L", -1.0)
    return ("P", 0.0)


def main():
    conn = sqlite3.connect(CFG.DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    now = utc_now_iso()

    br_cols = set(table_cols(conn, "bet_recommendations"))
    games_cols = set(table_cols(conn, "games"))
    tm_cols = set(table_cols(conn, "team_map_oddsapi"))

    # ----------------------------
    # 1) Backfill team_ids if missing
    # ----------------------------
    if {"home_team_id", "away_team_id"}.issubset(br_cols) and {"team_id", "oddsapi_team_name"}.issubset(tm_cols):
        conn.execute(
            """
            UPDATE bet_recommendations
            SET home_team_id = (
              SELECT tm.team_id
              FROM team_map_oddsapi tm
              WHERE tm.oddsapi_team_name = bet_recommendations.home_team
              LIMIT 1
            )
            WHERE home_team_id IS NULL AND home_team IS NOT NULL
            """
        )
        conn.execute(
            """
            UPDATE bet_recommendations
            SET away_team_id = (
              SELECT tm.team_id
              FROM team_map_oddsapi tm
              WHERE tm.oddsapi_team_name = bet_recommendations.away_team
              LIMIT 1
            )
            WHERE away_team_id IS NULL AND away_team IS NOT NULL
            """
        )

    # ----------------------------
    # 2) Attach game_id (best effort)
    # ----------------------------
    can_attach_game_id = (
        "game_id" in br_cols
        and {"game_id", "home_team_id", "away_team_id", "game_date"}.issubset(br_cols)
        and {"game_id", "home_team_id", "away_team_id", "game_date"}.issubset(games_cols)
    )

    matched = 0

    if can_attach_game_id:
        candidates = conn.execute(
            """
            SELECT rec_id, game_date, home_team_id, away_team_id
            FROM bet_recommendations
            WHERE qualifies_bet = 1
              AND graded_at_utc IS NULL
              AND game_id IS NULL
              AND home_team_id IS NOT NULL
              AND away_team_id IS NOT NULL
              AND game_date IS NOT NULL
            """
        ).fetchall()

        for rec_id, game_date, home_id, away_id in candidates:
            row = conn.execute(
                """
                SELECT g.game_id
                FROM games g
                WHERE g.home_team_id = ?
                  AND g.away_team_id = ?
                  AND date(g.game_date) BETWEEN date(?, '-1 day') AND date(?, '+1 day')
                ORDER BY
                  ABS(julianday(date(g.game_date)) - julianday(date(?))) ASC,
                  g.game_id DESC
                LIMIT 1
                """,
                (home_id, away_id, game_date, game_date, game_date),
            ).fetchone()

            if row and row[0] is not None:
                conn.execute(
                    "UPDATE bet_recommendations SET game_id=? WHERE rec_id=?",
                    (int(row[0]), int(rec_id))
                )
                matched += 1

    # ----------------------------
    # 3) Pull bets to grade (latest per matchup only)
    # ----------------------------
    updated = 0

    to_grade = conn.execute(
        """
        WITH latest_per_matchup AS (
          SELECT
            bookmaker,
            game_date,
            home_team,
            away_team,
            MAX(run_utc) AS max_run_utc
          FROM bet_recommendations
          WHERE qualifies_bet = 1
            AND graded_at_utc IS NULL
            AND game_date IS NOT NULL
          GROUP BY bookmaker, game_date, home_team, away_team
        )
        SELECT br.rec_id,
               br.bookmaker,
               br.game_date,
               br.home_team, br.away_team,
               br.home_spread,
               br.pick_side,
               br.stake_units,
               br.game_id,
               br.home_team_id,
               br.away_team_id
        FROM bet_recommendations br
        JOIN latest_per_matchup l
          ON l.bookmaker = br.bookmaker
         AND l.game_date = br.game_date
         AND l.home_team = br.home_team
         AND l.away_team = br.away_team
         AND l.max_run_utc = br.run_utc
        """
    ).fetchall()

    # preload finished games for fast lookup fallback
    if not {"home_score", "away_score", "game_date"}.issubset(games_cols):
        raise RuntimeError("games table missing required columns")

    games_rows = conn.execute(
        """
        SELECT game_id, date(game_date), home_team_id, away_team_id, home_score, away_score
        FROM games
        WHERE home_score IS NOT NULL AND away_score IS NOT NULL
        """
    ).fetchall()

    games_idx = {
        (d, int(hid), int(aid)): (int(hs), int(a_s), int(gid))
        for gid, d, hid, aid, hs, a_s in games_rows
    }

    for rec_id, bookmaker, game_date, home_team, away_team, home_spread, pick_side, stake_units, game_id, hid, aid in to_grade:
        hs = None
        a_s = None

        # Preferred: use game_id
        if game_id is not None:
            row = conn.execute(
                "SELECT home_score, away_score FROM games WHERE game_id=?",
                (int(game_id),),
            ).fetchone()
            if row and row[0] is not None and row[1] is not None:
                hs, a_s = int(row[0]), int(row[1])

        # Fallback: match via (date, team_ids)
        if (hs is None or a_s is None) and hid is not None and aid is not None:
            key = (str(game_date), int(hid), int(aid))
            if key in games_idx:
                hs, a_s, gid2 = games_idx[key]
                if game_id is None and can_attach_game_id:
                    conn.execute(
                        "UPDATE bet_recommendations SET game_id=? WHERE rec_id=?",
                        (int(gid2), int(rec_id))
                    )

        # skip if game not found / not finished
        if hs is None or a_s is None:
            continue

        ats, profit_per_unit = grade_ats(hs, a_s, home_spread, pick_side)

        profit_total = (
            float(profit_per_unit) * float(stake_units)
            if profit_per_unit is not None and stake_units is not None
            else profit_per_unit
        )

        conn.execute(
            """
            UPDATE bet_recommendations
            SET final_home_score=?,
                final_away_score=?,
                ats_result=?,
                profit_units=?,
                graded_at_utc=?
            WHERE rec_id=?
            """,
            (int(hs), int(a_s), ats, profit_total, now, int(rec_id)),
        )

        updated += 1

    conn.commit()

    # ----------------------------
    # 4) Summary output
    # ----------------------------
    summary = conn.execute(
        """
        SELECT
          COUNT(*),
          SUM(CASE WHEN ats_result='W' THEN 1 ELSE 0 END),
          SUM(CASE WHEN ats_result='L' THEN 1 ELSE 0 END),
          SUM(CASE WHEN ats_result='P' THEN 1 ELSE 0 END),
          ROUND(SUM(COALESCE(profit_units,0)), 4)
        FROM bet_recommendations
        WHERE qualifies_bet=1 AND graded_at_utc IS NOT NULL
        """
    ).fetchone()

    unmatched = conn.execute(
        """
        SELECT COUNT(*)
        FROM bet_recommendations
        WHERE qualifies_bet=1 AND graded_at_utc IS NULL
        """
    ).fetchone()[0]

    conn.close()

    print("===================================")
    print("GRADE RECOMMENDATIONS")
    print("Graded at:", now)
    print("Matched game_id this run:", matched)
    print("Newly graded this run:", updated)
    print("Total graded bets:", summary[0],
          "W-L-P:", summary[1], "-", summary[2], "-", summary[3],
          "Units:", summary[4])
    print("Still-ungraded qualifying bets:", unmatched)
    print("===================================")


if __name__ == "__main__":
    main()