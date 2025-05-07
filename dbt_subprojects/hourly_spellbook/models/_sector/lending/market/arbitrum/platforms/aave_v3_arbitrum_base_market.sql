{{
  config(
    schema = 'aave_v3_arbitrum',
    alias = 'base_market',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  lending_aave_v3_compatible_market(
    blockchain = 'arbitrum',
    decoded_contract_name = 'L2Pool'
  )
}}
