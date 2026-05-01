{{
 config(
       schema = 'meteora_v1_solana',
       alias = 'base_trades',
       materialized = 'view'
       )
}}

select * from {{ ref('meteora_v1_solana_base_trades_backfill') }}
