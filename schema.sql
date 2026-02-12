CREATE TABLE teams (
    id INT PRIMARY KEY,
    abbreviation TEXT NOT NULL,
    team_name TEXT NOT NULL -- e.g., "Sacramento Kings"
);

CREATE TABLE games (
    game_id TEXT PRIMARY KEY,
    season_id INT NOT NULL,
    game_date DATE NOT NULL
);

CREATE TABLE team_game_stats (
    -- Composite Key: A specific team's stats for a specific game
    game_id TEXT,
    team_id INT,

    matchup TEXT, -- "SAC @ NOP" (Crucial for Gold Layer derivation)
    wl CHAR(1),   -- "W" or "L"

    min INT,
    pts INT,

    fgm INT,
    fga INT,
    fg_pct NUMERIC(5,3),

    fg3m INT,
    fg3a INT,
    fg3_pct NUMERIC(5,3),

    ftm INT,
    fta INT,
    ft_pct NUMERIC(5,3),

    oreb INT,
    dreb INT,
    reb INT,

    ast INT,
    stl INT,
    blk INT,
    tov INT,
    pf INT,

    plus_minus INT,

    PRIMARY KEY (game_id, team_id)
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (team_id) REFERENCES teams(id)
);

CREATE TABLE players (
    player_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS play_by_play (
    game_id TEXT,
    eventnum INT,
    eventmsgtype INT,
    period INT,
    pctimestring TEXT,
    homedescription TEXT,
    visitordescription TEXT,
    score TEXT,
    player1_id INT,
    player2_id INT,
    player3_id INT,
    PRIMARY KEY (game_id, eventnum)
);

-- Add index for fast "Clutch Time" lookups
CREATE INDEX idx_pbp_game ON play_by_play(game_id);


