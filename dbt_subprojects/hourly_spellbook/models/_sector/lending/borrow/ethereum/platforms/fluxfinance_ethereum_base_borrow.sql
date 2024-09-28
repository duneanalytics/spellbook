{{
  config(
    schema = 'fluxfinance_ethereum',
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
    {'contract': 'fUSDC'},
    {'contract': 'fDAI'},
    {'contract': 'fUSDT'},
    {'contract': 'fFRAX'},
    {'contract': 'fOUSG'},
  ]
%}

{{
  lending_compound_v2_compatible_borrow(
    blockchain = 'ethereum',
    project = 'fluxfinance',
    version = '1',
    decoded_project = 'fluxfinance',
    sources = config_sources
  )
}}
