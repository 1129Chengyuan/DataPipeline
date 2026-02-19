-- stg_teams: clean team dimension
-- Materialized: view

with source as (
    select * from {{ source('nba_gold', 'teams') }}
)

select
    id                                              as team_id,
    abbreviation,
    team_name

from source
