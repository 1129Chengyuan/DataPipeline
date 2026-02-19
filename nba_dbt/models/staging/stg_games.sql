-- stg_games: clean game dimension with season label
-- Materialized: view

with source as (
    select * from {{ source('nba_gold', 'games') }}
)

select
    game_id,
    season_id                                       as season,
    game_date,
    extract(year from game_date)                    as game_year,
    extract(month from game_date)                   as game_month,
    to_char(game_date, 'Day')                       as day_of_week,

    -- Season label (e.g. "2023-24")
    season_id || '-' || right((season_id + 1)::text, 2)  as season_label

from source
