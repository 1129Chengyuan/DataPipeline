-- stg_player_stats: player advanced box score stats with names
-- Materialized: view

with source as (
    select * from {{ source('nba_gold', 'fact_player_stats') }}
)

select
    s.game_id,
    s.player_id,
    s.team_id,
    s.start_position,
    s.comment,
    s.minutes,

    -- Advanced stats
    s.off_rating,
    s.def_rating,
    s.net_rating,
    s.usg_pct,
    s.ts_pct,
    s.efg_pct,
    s.pie,

    -- Flag: did this player actually play?
    case
        when s.minutes is not null and s.minutes > 0 then true
        else false
    end                                             as did_play

from source s
