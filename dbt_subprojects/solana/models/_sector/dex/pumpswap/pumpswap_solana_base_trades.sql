{{
  config(
    schema = 'pumpswap_solana'
    , alias = 'base_trades'
    , materialized = 'view'
  )
}}

select * from {{ ref('pumpswap_solana_base_trades_backfill') }}
