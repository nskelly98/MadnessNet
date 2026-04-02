PRAGMA foreign_keys = ON;

-- One row per odds pull, per game, per bookmaker
CREATE TABLE IF NOT EXISTS odds_spreads_snapshots (
  snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  pulled_at_utc    TEXT NOT NULL,             -- ISO UTC
  game_date        TEXT,                      -- YYYY-MM-DD (local game day from API if present)
  commence_time_utc TEXT,                     -- from odds api
  bookmaker        TEXT NOT NULL,             -- "fanduel"
  home_team        TEXT NOT NULL,
  away_team        TEXT NOT NULL,
  home_spread      REAL,
  away_spread      REAL,
  home_price       INTEGER,
  away_price       INTEGER,
  source_event_id  TEXT,                      -- odds api event id
  source_game_id   TEXT                       -- odds api game id if present
);

CREATE INDEX IF NOT EXISTS idx_odds_snapshots_pulled ON odds_spreads_snapshots(pulled_at_utc);
CREATE INDEX IF NOT EXISTS idx_odds_snapshots_teams ON odds_spreads_snapshots(home_team, away_team, commence_time_utc);

-- One row per model-scored game per run (log EVERYTHING scored, not just bets)
CREATE TABLE IF NOT EXISTS bet_recommendations (
  rec_id            INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id            TEXT NOT NULL,            -- e.g., 2026-01-27T15:47:31Z
  run_utc           TEXT NOT NULL,            -- same as run_id, stored separately for indexing
  game_date         TEXT,                     -- YYYY-MM-DD (from odds/game)
  commence_time_utc TEXT,                     -- from odds api
  bookmaker         TEXT NOT NULL,            -- "fanduel"

  home_team         TEXT NOT NULL,
  away_team         TEXT NOT NULL,

  -- Mapping / joins
  game_id           INTEGER,                  -- games.game_id (if joined)
  odds_snapshot_id  INTEGER,                  -- odds_spreads_snapshots.snapshot_id at time of run

  -- Line at time of pick (what you actually bet into)
  home_spread       REAL,
  away_spread       REAL,

  -- Model predictions (home margin: + means home wins by X)
  pred_home_ridge   REAL,
  pred_home_xgb     REAL,

  -- Edges (home edge: pred - line for home, standardized so bigger positive = better)
  edge_home_ridge   REAL,
  edge_home_xgb     REAL,

  -- Decision
  pick_side         TEXT,                     -- "HOME" or "AWAY" or NULL if no bet
  model_primary     TEXT NOT NULL,            -- "ridge" (your betting model)
  edge_primary      REAL,                     -- edge used for bet decision (after caps)
  qualifies_bet     INTEGER NOT NULL DEFAULT 0, -- 1 if bet placed per rules
  stake_units       REAL NOT NULL DEFAULT 1.0,

  -- Grading fields (filled by night job)
  final_home_score  INTEGER,
  final_away_score  INTEGER,
  ats_result        TEXT,                     -- "W","L","P","N/A"
  profit_units      REAL,                     -- +0.91/-1.0 typical (if -110)
  graded_at_utc     TEXT,

  -- CLV-ish (optional: latest line you pulled later)
  latest_home_spread REAL,
  latest_away_spread REAL,
  clv_points        REAL,                     -- standardized in your favor (positive good)

  notes             TEXT,

  UNIQUE(run_id, bookmaker, home_team, away_team, commence_time_utc)
);

CREATE INDEX IF NOT EXISTS idx_recs_run ON bet_recommendations(run_utc);
CREATE INDEX IF NOT EXISTS idx_recs_game ON bet_recommendations(home_team, away_team, commence_time_utc);
CREATE INDEX IF NOT EXISTS idx_recs_qualifies ON bet_recommendations(qualifies_bet);
