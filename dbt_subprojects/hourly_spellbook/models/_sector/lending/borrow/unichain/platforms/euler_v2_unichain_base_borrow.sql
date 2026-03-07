{{
  config(
    schema = 'euler_v2_unichain',
    alias = 'base_borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  lending_euler_v2_compatible_borrow(
    blockchain = 'unichain',
    project = 'euler',
    version = '2',
    decoded_contract_name = 'EVault'
  )
}}
