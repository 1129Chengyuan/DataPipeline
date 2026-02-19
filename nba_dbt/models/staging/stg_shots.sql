-- stg_shots: shot chart data with player/team context
-- Materialized: view

with source as (
    select * from {{ source('nba_gold', 'fact_shots') }}
)

select
    s.game_id,
    s.game_event_id,
    s.player_id,
    s.team_id,
    s.period,

    -- Spatial
    s.loc_x,
    s.loc_y,
    s.shot_distance,

    -- Outcome
    s.shot_attempted_flag,
    s.shot_made_flag,

    -- Zone classification
    s.shot_zone_basic,
    s.shot_zone_area,
    s.shot_zone_range,

    -- Derived: simplified zone for dashboards
    case
        when s.shot_zone_basic = 'Restricted Area'         then 'Paint'
        when s.shot_zone_basic = 'In The Paint (Non-RA)'   then 'Paint'
        when s.shot_zone_basic = 'Mid-Range'               then 'Mid-Range'
        when s.shot_zone_basic = 'Above the Break 3'       then 'Three'
        when s.shot_zone_basic = 'Left Corner 3'           then 'Corner Three'
        when s.shot_zone_basic = 'Right Corner 3'          then 'Corner Three'
        when s.shot_zone_basic = 'Backcourt'               then 'Backcourt'
        else 'Other'
    end                                             as shot_zone_group

from source s
