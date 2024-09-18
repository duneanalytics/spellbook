{{
  config(
    schema = 'aave_etherfi_v3_ethereum',
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
    blockchain = 'ethereum',
    project = 'aave_etherfi',
    version = '3',
    project_decoded_as = 'aave_v3_etherfi',
    decoded_contract_name = 'PoolInstance'
  )
}}
