-- fct_player_shooting_splits
-- Per-player shooting efficiency by zone and season.
-- Materialized: table
--
-- Resume patterns: GROUP BY, conditional aggregation, percentage calculations

with shot_data as (
    select
        s.player_id,
        g.season,
        s.shot_zone_basic,
        s.shot_zone_area,
        s.shot_zone_range,
        s.shot_zone_group,
        s.shot_attempted_flag,
        s.shot_made_flag,
        s.shot_distance
    from {{ ref('stg_shots') }}  s
    join {{ ref('stg_games') }}  g  on s.game_id = g.game_id
)

select
    sd.player_id,
    p.full_name,
    sd.season,
    sd.shot_zone_group,
    sd.shot_zone_basic,

    -- Volume
    count(*)                                                              as attempts,
    sum(case when sd.shot_made_flag then 1 else 0 end)                    as makes,

    -- Accuracy
    round(
        sum(case when sd.shot_made_flag then 1 else 0 end)::numeric
        / nullif(count(*), 0), 3
    )                                                                     as fg_pct,

    -- Distance
    round(avg(sd.shot_distance), 1)                                       as avg_distance,

    -- Share of total attempts for this player-season
    round(
        count(*)::numeric / nullif(
            sum(count(*)) over (partition by sd.player_id, sd.season), 0
        ), 3
    )                                                                     as shot_distribution_pct

from shot_data sd
join {{ ref('stg_players') }} p on sd.player_id = p.player_id
group by sd.player_id, p.full_name, sd.season, sd.shot_zone_group, sd.shot_zone_basic
having count(*) >= 5  -- filter noise: at least 5 attempts in this zone
