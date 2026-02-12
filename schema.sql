-- 1. TEAMS
-- Reduced to exactly what is in the JSON. 
-- 'city' and 'nickname' are combined in 'team_name' and require parsing (Gold Layer).
-- 'year_founded' is removed as it is not in the source.
CREATE TABLE teams (
    id INT PRIMARY KEY,
    abbreviation TEXT NOT NULL,
    team_name TEXT NOT NULL -- e.g., "Sacramento Kings"
);

-- 2. GAMES
-- Removed 'home_team_id' and 'away_team_id'.
-- In this data source, you don't know the Opponent ID, only the Opponent Abbreviation (from Matchup).
-- Resolving IDs requires a lookup that belongs in the Gold Layer.
CREATE TABLE games (
    game_id TEXT PRIMARY KEY,
    season_id INT NOT NULL,
    game_date DATE NOT NULL
    -- Foreign keys removed for Silver to allow faster, decoupled loading
);

-- 3. TEAM GAME STATS (The "Fact" Table)
-- Added 'matchup' here because it is specific to the team's perspective (e.g., "SAC @ NOP").
-- All stats remain 1:1 with the headers.
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
