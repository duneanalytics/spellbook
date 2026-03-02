{{
 config(
       schema = 'raydium_v5',
       alias = 'base_trades',
       materialized = 'view'
       )
}}

select * from {{ ref('raydium_v5_base_trades_backfill') }}
