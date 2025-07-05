{{
  config(
    schema = 'pike_sonic',
    alias = 'base_borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  lending_pike_compatible_borrow(
    blockchain = 'sonic',
    project = 'pike',
    version = '1',
    evt_borrow_table = 'ptoken_evt_borrow',
    evt_repay_table = 'ptoken_evt_repayborrow',
    evt_liquidation_borrow_table = 'ptoken_evt_liquidateborrow',
    deploy_market_table = 'factory_call_deploymarket'
  )
}}