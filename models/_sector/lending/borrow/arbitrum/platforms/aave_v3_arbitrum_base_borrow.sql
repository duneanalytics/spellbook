{{
  config(
    schema = 'aave_v3_arbitrum',
    alias = 'base_borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['token_address', 'evt_tx_hash', 'evt_block_number', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  lending_aave_v3_compatible_borrow(
    blockchain = 'arbitrum',
    project = 'aave',
    version = '3'
  )
}}
