{{
  config(
    schema = 'layer_bank_scroll',
    alias = 'base_borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{%
  set config_sources = [
    {'contract': 'lETH'},
    {'contract': 'lUSDC'},
    {'contract': 'lwstETH'},
  ]
%}

{{
  lending_compound_v2_compatible_borrow(
    blockchain = 'scroll',
    project = 'layer_bank',
    version = '1',
    decoded_project = 'layer_bank',
    sources = config_sources
  )
}}
