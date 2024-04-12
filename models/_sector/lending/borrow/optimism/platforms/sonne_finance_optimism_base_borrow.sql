{{
  config(
    schema = 'sonne_finance_optimism',
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
    {'contract': 'soDAI'},
    {'contract': 'soMAI'},
    {'contract': 'soOP'},
    {'contract': 'soSNX'},
    {'contract': 'soSUSD'},
    {'contract': 'soUSDC'},
    {'contract': 'soUSDT'},
    {'contract': 'soWBTC'},
    {'contract': 'soWETH'},
    {'contract': 'sowstETH'},
  ]
%}

{{
  lending_compound_v2_compatible_borrow(
    blockchain = 'optimism',
    project = 'sonne_finance',
    version = '1',
    decoded_project = 'sonne_finance',
    sources = config_sources
  )
}}
