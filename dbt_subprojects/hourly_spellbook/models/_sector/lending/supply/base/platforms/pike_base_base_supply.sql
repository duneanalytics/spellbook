{{
  config(
    schema = 'pike_base',
    alias = 'base_supply',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  lending_pike_compatible_supply(
    blockchain = 'base',
    project = 'pike',
    version = '1',
    deploy_market_table = 'factory_call_deploymarket',
    evt_transfer_table = 'ptoken_evt_transfer'
  )
}}
