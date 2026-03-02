{{
  config(
    schema = 'orca_whirlpool_v2'
    , alias = 'base_trades'
    , materialized = 'view'
  )
}}

select * from {{ ref('orca_whirlpool_v2_base_trades_backfill') }}
