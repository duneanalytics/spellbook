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
    {'contract': 'lETH', 'borrower_column_name': 'account', 'borrowAmount_column_name': 'ammount', 'repayAmount_column_name': 'amount'},
    {'contract': 'lUSDC', 'borrower_column_name': 'account', 'borrowAmount_column_name': 'ammount', 'repayAmount_column_name': 'amount'},
    {'contract': 'lwstETH', 'borrower_column_name': 'account', 'borrowAmount_column_name': 'ammount', 'repayAmount_column_name': 'amount'},
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
