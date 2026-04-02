DROP TABLE IF EXISTS teams;
CREATE TABLE teams (
  team_id INTEGER PRIMARY KEY,
  team_name TEXT NOT NULL
);

DROP TABLE IF EXISTS games;
CREATE TABLE games (
  game_id INTEGER PRIMARY KEY,
  game_date TEXT NOT NULL,
  home_team_id INTEGER NOT NULL,
  away_team_id INTEGER NOT NULL,
  home_score INTEGER,
  away_score INTEGER
);

DROP TABLE IF EXISTS team_game_stats;
CREATE TABLE team_game_stats (
  team_id INTEGER NOT NULL,
  game_id INTEGER NOT NULL,
  off_eff REAL,
  def_eff REAL,
  PRIMARY KEY (team_id, game_id)
);

DROP TABLE IF EXISTS team_rolling_stats;
CREATE TABLE team_rolling_stats (
  team_id INTEGER NOT NULL,
  game_id INTEGER NOT NULL,
  roll5_adj_em REAL,
  roll10_adj_em REAL,
  ewm_adj_em REAL,
  roll5_off_eff REAL,
  roll10_def_eff REAL,
  trend_adj_em REAL,
  PRIMARY KEY (team_id, game_id)
);

/* model_games_enriched is rebuilt by script; schema can be created by pandas to_sql */
DROP TABLE IF EXISTS model_games_enriched;
