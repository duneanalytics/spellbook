{{
  config(
    schema = 'aave_v2_ethereum',
    alias = 'base_borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  lending_aave_v2_compatible_borrow(
    blockchain = 'ethereum',
    project = 'aave',
    version = '2'
  )
}}
