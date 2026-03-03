{{
 config(
       schema = 'raydium_v3',
       alias = 'base_trades',
       materialized = 'view'
       )
}}

select * from {{ ref('raydium_v3_base_trades_backfill') }}
