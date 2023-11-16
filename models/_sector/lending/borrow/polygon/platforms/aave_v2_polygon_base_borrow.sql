{{
  config(
    schema = 'aave_v2_polygon',
    alias = 'base_borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['token_address', 'evt_tx_hash', 'evt_block_number', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  lending_aave_v2_compatible_borrow(
    blockchain = 'polygon',
    project = 'aave',
    version = '2'
  )
}}
