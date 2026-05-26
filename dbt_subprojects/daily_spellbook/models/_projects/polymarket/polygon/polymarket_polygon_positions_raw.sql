{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions_raw',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_address', 'token_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

with balances as (
    select
        blockchain
        , day
        , address
        , token_address
        , token_id
        , balance_raw
        , last_updated
    from {{ ref('polymarket_polygon_positions_balances_repro') }}
)

select
    blockchain
    , day
    , address
    , token_address
    , token_id
    , balance_raw
    , balance_raw / 1e6 as balance
    , last_updated
from balances
