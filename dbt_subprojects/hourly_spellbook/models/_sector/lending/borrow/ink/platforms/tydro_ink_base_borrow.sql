{{
  config(
    schema = 'tydro_ink',
    alias = 'base_borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  lending_aave_v3_compatible_borrow(
    blockchain = 'ink',
    project = 'tydro',
    version = '1',
    project_decoded_as = 'aave_v3',
    decoded_contract_name = 'L2Pool'
  )
}}
