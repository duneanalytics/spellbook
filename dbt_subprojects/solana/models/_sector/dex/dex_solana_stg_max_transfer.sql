{{
    config(
        schema = 'dex_solana_stg'
        , alias = 'max_transfer'
        , materialized = 'table'
    )
}}

select
    max(block_date) as max_block_date
    , max(block_time) as max_block_time
    , max(block_slot) as max_block_slot
from
    {{ source('tokens_solana', 'transfers') }}
where
    -- one week buffer to prevent full scan
    block_date >= now() - interval '7' day