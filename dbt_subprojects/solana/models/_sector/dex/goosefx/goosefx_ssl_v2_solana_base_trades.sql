{{
 config(
       schema = 'goosefx_ssl_v2_solana',
       alias = 'base_trades',
       materialized = 'view'
       )
}}

select * from {{ ref('goosefx_ssl_v2_solana_base_trades_backfill') }}