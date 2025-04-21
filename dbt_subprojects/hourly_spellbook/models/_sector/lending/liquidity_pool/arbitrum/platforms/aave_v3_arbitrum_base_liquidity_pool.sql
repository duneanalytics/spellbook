{{
  config(
    schema = 'aave_v3_arbitrum',
    alias = 'base_liquidity_pool',
    materialized = 'view',
    unique_key = ['block_date', 'wallet_address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  lending_aave_v3_compatible_liquidity_pool(
    blockchain = 'arbitrum',
    project = 'aave',
    version = '3',
    decoded_contract_name = 'L2Pool'
  )
}}
