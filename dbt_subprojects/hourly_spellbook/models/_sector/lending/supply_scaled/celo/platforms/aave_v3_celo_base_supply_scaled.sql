{{
  config(
    schema = 'aave_v3_celo',
    alias = 'base_supply_scaled',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    unique_key = ['block_hour', 'token_address', 'user'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_hour')]
  )
}}

{{
  lending_aave_v3_compatible_supply_scaled(
    blockchain = 'celo',
    project = 'aave',
    version = '3'
  )
}} 