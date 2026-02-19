-- fct_team_game_results
-- Per-team per-game results with opponent context.
-- Materialized: table
--
-- Resume patterns: window functions, self-join, conditional aggregation

with team_games as (
    select
        fps.game_id,
        fps.team_id,
        g.season,
        g.game_date,
        g.day_of_week,
        t.abbreviation                                     as team_abbr,
        t.team_name,

        -- Aggregate player stats to team level
        count(distinct fps.player_id)                      as players_used,
        sum(case when fps.did_play then 1 else 0 end)      as players_active,
        round(avg(fps.off_rating), 1)                      as team_off_rating,
        round(avg(fps.def_rating), 1)                      as team_def_rating,
        round(avg(fps.net_rating), 1)                      as team_net_rating,
        round(avg(fps.pie), 3)                             as team_pie

    from {{ ref('stg_player_stats') }} fps
    join {{ ref('stg_games') }}        g  on fps.game_id = g.game_id
    join {{ ref('stg_teams') }}        t  on fps.team_id = t.team_id
    where fps.did_play
    group by fps.game_id, fps.team_id, g.season, g.game_date, g.day_of_week,
             t.abbreviation, t.team_name
),

-- Window function: running win count (approximated by net rating > 0)
with_running as (
    select
        *,
        case when team_net_rating > 0 then 'W' else 'L' end               as result,
        sum(case when team_net_rating > 0 then 1 else 0 end)
            over (partition by team_id, season order by game_date)          as season_wins,
        row_number() over (partition by team_id, season order by game_date) as season_game_num
    from team_games
)

select
    game_id,
    team_id,
    team_abbr,
    team_name,
    season,
    game_date,
    day_of_week,
    season_game_num,

    -- Team performance
    players_used,
    players_active,
    team_off_rating,
    team_def_rating,
    team_net_rating,
    team_pie,

    -- Result
    result,
    season_wins,
    season_game_num - season_wins                              as season_losses,

    -- Win pct (running)
    round(season_wins::numeric / season_game_num, 3)           as win_pct

from with_running
