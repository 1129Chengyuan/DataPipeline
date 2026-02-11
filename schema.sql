CREATE TABLE IF NOT EXISTS teams (
    id INT PRIMARY KEY,
    full_name TEXT,
    abbreviation TEXT,
    nickname TEXT,
    city TEXT,
    state TEXT,
    year_founded INT
);

CREATE TABLE IF NOT EXISTS players (
    id INT PRIMARY KEY,
    full_name TEXT,
    first_name TEXT,
    last_name TEXT,
    is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS games (
    game_id TEXT,
    team_id INT,
    game_date DATE,
    matchup TEXT,
    wl TEXT,
    pts INT,
    ast INT,
    reb INT,
    PRIMARY KEY (game_id, team_id),
    FOREIGN KEY (team_id) REFERENCES teams(id)
);
