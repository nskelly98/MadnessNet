-- Odds API + betting automation tables

DROP TABLE IF EXISTS oddsapi_odds_snapshots;
CREATE TABLE oddsapi_odds_snapshots (
  pulled_at_utc TEXT NOT NULL,
  sport_key TEXT NOT NULL,
  event_id TEXT NOT NULL,
  commence_time TEXT,
  home_team TEXT,
  away_team TEXT,
  bookmaker TEXT NOT NULL,
  market TEXT NOT NULL,
  spread_home REAL,
  price_home INTEGER,
  spread_away REAL,
  price_away INTEGER,
  last_update TEXT,
  raw_json TEXT,
  PRIMARY KEY (pulled_at_utc, event_id, bookmaker, market)
);

DROP TABLE IF EXISTS team_map_oddsapi;
CREATE TABLE team_map_oddsapi (
  team_id INTEGER PRIMARY KEY,
  team_name_db TEXT NOT NULL,
  oddsapi_team_name TEXT,
  match_score INTEGER,
  match_method TEXT
);

DROP TABLE IF EXISTS daily_bets;
CREATE TABLE daily_bets (
  run_at_utc TEXT NOT NULL,
  game_date TEXT NOT NULL,
  event_id TEXT NOT NULL,
  home_team TEXT NOT NULL,
  away_team TEXT NOT NULL,
  bookmaker TEXT NOT NULL,
  spread_home_req REAL NOT NULL,
  pred_margin REAL NOT NULL,
  edge REAL NOT NULL,
  bet_side TEXT NOT NULL,          -- HOME / AWAY / NO_BET
  bet_threshold REAL NOT NULL,
  model_version TEXT NOT NULL,
  PRIMARY KEY (run_at_utc, event_id, bookmaker)
);

DROP TABLE IF EXISTS bet_results;
CREATE TABLE bet_results (
  event_id TEXT NOT NULL,
  bookmaker TEXT NOT NULL,
  home_score INTEGER,
  away_score INTEGER,
  home_margin REAL,
  result TEXT,    -- WIN / LOSS / PUSH / NO_BET
  graded_at_utc TEXT NOT NULL,
  PRIMARY KEY (event_id, bookmaker)
);

DROP TABLE IF EXISTS daily_model_predictions;
CREATE TABLE daily_model_predictions (
  run_at_utc TEXT NOT NULL,
  event_id TEXT NOT NULL,
  bookmaker TEXT NOT NULL,
  model_name TEXT NOT NULL,         -- 'ridge' or 'xgb'
  pred_margin REAL NOT NULL,
  PRIMARY KEY (run_at_utc, event_id, bookmaker, model_name)
);
