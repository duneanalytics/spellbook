{{
 config(
       schema = 'goosefx_ssl_v2_solana',
       alias = 'base_trades',
       materialized = 'view'
       )
}}

SELECT * FROM {{ ref('goosefx_ssl_v2_solana_base_trades_backfill') }}
UNION ALL
SELECT * FROM {{ ref('goosefx_ssl_v2_solana_base_trades_current') }}
