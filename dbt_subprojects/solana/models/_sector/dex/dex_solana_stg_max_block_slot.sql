{{
    config(
        schema = 'dex_solana'
        , alias = 'stg_max_block_slot'
        , materialized = 'table'
    )
}}
-- depends_on: {{ ref('dex_solana_base_trades') }}

select
    max(block_date) as block_date_filter
    , max(block_time) as block_time_filter
    , max(block_slot) as block_slot_filter
from
    {{ source('tokens_solana', 'transfers') }}
where
    -- to prevent full scan, give short timeframe filter with some buffer
    block_date >= now() - interval '3' day