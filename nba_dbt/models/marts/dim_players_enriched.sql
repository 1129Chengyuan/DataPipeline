-- dim_players_enriched
-- Player dimension with career context derived from fact tables.
-- Materialized: table
--
-- Resume patterns: LEFT JOIN, aggregation, date functions

with career_stats as (
    select
        ps.player_id,
        count(distinct ps.game_id)                                as total_games,
        count(distinct case when ps.did_play then ps.game_id end) as games_played,
        min(g.game_date)                                          as career_first_game,
        max(g.game_date)                                          as career_last_game,
        count(distinct g.season)                                  as seasons_played,

        -- Career averages (games played only)
        round(avg(case when ps.did_play then ps.minutes end), 1)  as career_avg_minutes,
        round(avg(case when ps.did_play then ps.off_rating end), 1) as career_avg_off_rtg,
        round(avg(case when ps.did_play then ps.def_rating end), 1) as career_avg_def_rtg,
        round(avg(case when ps.did_play then ps.pie end), 3)      as career_avg_pie

    from {{ ref('stg_player_stats') }} ps
    join {{ ref('stg_games') }}        g  on ps.game_id = g.game_id
    group by ps.player_id
),

shooting_stats as (
    select
        player_id,
        count(*)                                                  as career_shot_attempts,
        sum(case when shot_made_flag then 1 else 0 end)           as career_shot_makes,
        round(
            sum(case when shot_made_flag then 1 else 0 end)::numeric
            / nullif(count(*), 0), 3
        )                                                         as career_fg_pct
    from {{ ref('stg_shots') }}
    group by player_id
)

select
    p.player_id,
    p.full_name,
    p.first_name,
    p.last_name,
    p.is_active,

    -- Career span
    cs.career_first_game,
    cs.career_last_game,
    cs.career_last_game - cs.career_first_game                    as career_days_span,
    cs.seasons_played,

    -- Career volume
    coalesce(cs.total_games, 0)                                   as total_games,
    coalesce(cs.games_played, 0)                                  as games_played,

    -- Career averages
    cs.career_avg_minutes,
    cs.career_avg_off_rtg,
    cs.career_avg_def_rtg,
    cs.career_avg_pie,

    -- Shooting career
    coalesce(ss.career_shot_attempts, 0)                          as career_shot_attempts,
    coalesce(ss.career_shot_makes, 0)                             as career_shot_makes,
    ss.career_fg_pct

from {{ ref('stg_players') }}    p
left join career_stats            cs on p.player_id = cs.player_id
left join shooting_stats          ss on p.player_id = ss.player_id
