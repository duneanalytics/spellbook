{{
    config(
        schema = 'dex_solana'
        , alias = 'stg_min_max_block_slot'
        , materialized = 'table'
    )
}}

-- Calculate the minimum of the maximums per DEX
-- This ensures all DEXes are aligned to the slowest one
with dex_maxes as (
    select
        project
        , version
        , max(block_date) as max_block_date
        , max(block_time) as max_block_time
        , max(block_slot) as max_block_slot
    from
        {{ ref('dex_solana_base_trades') }}
    where
        true
        -- current partition only to prevent full scan
        and block_month = date_trunc('month', now())
        -- filter to DEXs that remain active
        and block_date >= now() - interval '3' day
    group by
        project
        , version
)
select
    min(max_block_date) as block_date_filter
    , min(max_block_time) as block_time_filter
    , min(max_block_slot) as block_slot_filter
from
    dex_maxes