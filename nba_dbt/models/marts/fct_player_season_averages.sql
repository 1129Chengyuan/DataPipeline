-- fct_player_season_averages
-- Per-player per-season averages — the classic "stat line" view.
-- Materialized: table
--
-- Resume patterns: GROUP BY aggregation, JOIN, CASE, ROUND

with player_games as (
    select
        ps.player_id,
        g.season,
        g.game_date,
        ps.minutes,
        ps.off_rating,
        ps.def_rating,
        ps.net_rating,
        ps.usg_pct,
        ps.ts_pct,
        ps.efg_pct,
        ps.pie,
        ps.did_play
    from {{ ref('stg_player_stats') }} ps
    join {{ ref('stg_games') }}        g  on ps.game_id = g.game_id
)

select
    pg.player_id,
    p.full_name,
    p.is_active,
    pg.season,

    -- Games
    count(*)                                                    as games_total,
    sum(case when pg.did_play then 1 else 0 end)                as games_played,

    -- Minutes
    round(avg(case when pg.did_play then pg.minutes end), 1)    as avg_minutes,

    -- Advanced averages (only for games played)
    round(avg(case when pg.did_play then pg.off_rating end), 1) as avg_off_rating,
    round(avg(case when pg.did_play then pg.def_rating end), 1) as avg_def_rating,
    round(avg(case when pg.did_play then pg.net_rating end), 1) as avg_net_rating,
    round(avg(case when pg.did_play then pg.usg_pct end), 3)    as avg_usg_pct,
    round(avg(case when pg.did_play then pg.ts_pct end), 3)     as avg_ts_pct,
    round(avg(case when pg.did_play then pg.efg_pct end), 3)    as avg_efg_pct,
    round(avg(case when pg.did_play then pg.pie end), 3)        as avg_pie,

    -- Season span
    min(pg.game_date)                                           as season_first_game,
    max(pg.game_date)                                           as season_last_game

from player_games pg
join {{ ref('stg_players') }} p on pg.player_id = p.player_id
group by pg.player_id, p.full_name, p.is_active, pg.season
having sum(case when pg.did_play then 1 else 0 end) > 0
