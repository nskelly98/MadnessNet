PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS teams;
CREATE TABLE teams (
  team_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  team_name   TEXT NOT NULL UNIQUE,
  conference  TEXT
);

DROP TABLE IF EXISTS games;
CREATE TABLE games (
  game_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  ncaa_game_id   TEXT UNIQUE,            -- the /game/{id} id from scoreboard url
  season         INTEGER NOT NULL,        -- season end-year label (e.g., 2026)
  game_date      TEXT NOT NULL,           -- YYYY-MM-DD
  home_team_id   INTEGER NOT NULL,
  away_team_id   INTEGER NOT NULL,
  neutral_site   INTEGER DEFAULT 0,
  home_score     INTEGER,
  away_score     INTEGER,
  UNIQUE(game_date, home_team_id, away_team_id),
  FOREIGN KEY(home_team_id) REFERENCES teams(team_id),
  FOREIGN KEY(away_team_id) REFERENCES teams(team_id)
);

DROP TABLE IF EXISTS team_game_stats;
CREATE TABLE team_game_stats (
  game_id       INTEGER NOT NULL,
  team_id       INTEGER NOT NULL,
  opponent_id   INTEGER,
  is_home       INTEGER,                 -- 1/0

  tempo         REAL,                    -- possessions estimate
  off_eff       REAL,                    -- points per 100 possessions
  def_eff       REAL,                    -- opponent points per 100 possessions

  -- placeholders for future KP-like
  adj_o         REAL,
  adj_d         REAL,
  adj_em        REAL,
  luck          REAL,
  sos_adj_em    REAL,

  -- boxscore-derived features (used later if you want)
  three_pct     REAL,
  two_pct       REAL,
  ft_pct        REAL,
  off_or_pct    REAL,
  def_or_pct    REAL,
  stl_pct       REAL,
  nst_pct       REAL,

  PRIMARY KEY (game_id, team_id),
  FOREIGN KEY(game_id) REFERENCES games(game_id),
  FOREIGN KEY(team_id) REFERENCES teams(team_id)
);

DROP TABLE IF EXISTS team_rolling_stats;
CREATE TABLE team_rolling_stats (
  team_id         INTEGER NOT NULL,
  game_id         INTEGER NOT NULL,
  roll5_adj_em    REAL,
  roll10_adj_em   REAL,
  ewm_adj_em      REAL,
  roll5_off_eff   REAL,
  roll10_def_eff  REAL,
  trend_adj_em    REAL,
  PRIMARY KEY(team_id, game_id)
);

DROP TABLE IF EXISTS model_games_enriched;

CREATE INDEX IF NOT EXISTS idx_games_date ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_games_season ON games(season);
CREATE INDEX IF NOT EXISTS idx_tgs_team ON team_game_stats(team_id);
CREATE INDEX IF NOT EXISTS idx_tgs_game ON team_game_stats(game_id);
