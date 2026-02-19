-- stg_players: clean player dimension
-- Materialized: view

with source as (
    select * from {{ source('nba_gold', 'players') }}
)

select
    player_id,
    first_name,
    last_name,
    first_name || ' ' || last_name                  as full_name,
    is_active

from source
