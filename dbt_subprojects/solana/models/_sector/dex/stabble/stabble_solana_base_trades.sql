{{
  config(
    schema = 'stabble_solana'
    , alias = 'base_trades'
    , materialized = 'view'
  )
}}

select * from {{ ref('stabble_solana_base_trades_backfill') }}
