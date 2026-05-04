{{
  config(
    schema = 'orca_whirlpool'
    , alias = 'base_trades'
    , materialized = 'view'
  )
}}

select * from {{ ref('orca_whirlpool_base_trades_backfill') }}
