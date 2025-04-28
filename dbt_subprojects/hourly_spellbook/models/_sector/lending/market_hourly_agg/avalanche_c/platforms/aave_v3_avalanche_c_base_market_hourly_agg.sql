{{
  config(
    schema = 'aave_v3_avalanche_c',
    alias = 'base_market_hourly_agg',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_hour', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_hour')]
  )
}}

{{
  lending_aave_v3_compatible_market_hourly_agg(
    blockchain = 'avalanche_c'
  )
}}
