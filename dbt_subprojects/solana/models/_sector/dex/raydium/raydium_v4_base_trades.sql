{{
  config(
    schema = 'raydium_v4'
    , alias = 'base_trades'
    , materialized = 'view'
  )
}}

select * from {{ ref('raydium_v4_base_trades_backfill') }}
