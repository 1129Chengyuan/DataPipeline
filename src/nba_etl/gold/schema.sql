-- NBA Data Warehouse Schema
-- Auto-runs on first Postgres container start via docker-entrypoint-initdb.d

-- 1. Dimensions
CREATE TABLE IF NOT EXISTS teams (
    id INT PRIMARY KEY,
    abbreviation TEXT,
    team_name TEXT
);

CREATE TABLE IF NOT EXISTS games (
    game_id TEXT PRIMARY KEY,
    season_id INT,
    game_date DATE
);

CREATE TABLE IF NOT EXISTS players (
    player_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    is_active BOOLEAN
);

-- 2. Fact: Player Advanced Stats per Game
CREATE TABLE IF NOT EXISTS fact_player_stats (
    game_id TEXT,
    player_id INT,
    team_id INT,

    start_position CHAR(3),
    comment TEXT,
    minutes NUMERIC(8,2),

    off_rating NUMERIC(8,3),
    def_rating NUMERIC(8,3),
    net_rating NUMERIC(8,3),
    usg_pct NUMERIC(8,3),
    ts_pct NUMERIC(8,3),
    efg_pct NUMERIC(8,3),
    pie NUMERIC(8,3),

    PRIMARY KEY (game_id, player_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(id)
);

-- 3. Fact: Team Game Logs
CREATE TABLE IF NOT EXISTS fact_team_stats (
    game_id TEXT,
    team_id INT,
    matchup TEXT,
    wl CHAR(1),

    minutes NUMERIC(8,2),
    pts INT,
    plus_minus INT,

    off_rating NUMERIC(8,3),
    def_rating NUMERIC(8,3),
    net_rating NUMERIC(8,3),
    pace NUMERIC(8,3),

    PRIMARY KEY (game_id, team_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (team_id) REFERENCES teams(id)
);

-- 4. Fact: Play by Play (V3)
CREATE TABLE IF NOT EXISTS fact_play_by_play (
    game_id TEXT,
    action_number INT,
    period INT,
    clock_seconds NUMERIC(10,2),
    team_id INT,
    player_id INT,

    action_type TEXT,
    sub_type TEXT,
    description TEXT,

    shot_distance INT,
    score_home INT,
    score_away INT,
    is_field_goal BOOLEAN,

    PRIMARY KEY (game_id, action_number),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);
CREATE INDEX IF NOT EXISTS idx_pbp_game_clock ON fact_play_by_play(game_id, period, clock_seconds);

-- 5. Fact: Shot Chart (Spatial Data)
CREATE TABLE IF NOT EXISTS fact_shots (
    game_id TEXT,
    game_event_id INT,
    player_id INT,
    team_id INT,
    period INT,

    loc_x NUMERIC(10,2),
    loc_y NUMERIC(10,2),
    shot_distance INT,

    shot_attempted_flag BOOLEAN,
    shot_made_flag BOOLEAN,
    shot_zone_basic TEXT,
    shot_zone_area TEXT,
    shot_zone_range TEXT,

    PRIMARY KEY (game_id, game_event_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id)
);
