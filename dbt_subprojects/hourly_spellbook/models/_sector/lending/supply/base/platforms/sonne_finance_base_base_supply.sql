{{
  config(
    schema = 'sonne_finance_base',
    alias = 'base_supply',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{%
  set config_sources = [
    {'contract': 'sobcbETH'},
    {'contract': 'sobDAI'},
    {'contract': 'sobUSDbC'},
    {'contract': 'sobUSDC'},
    {'contract': 'sobWETH'},
  ]
%}

{{
  lending_compound_v2_compatible_supply(
    blockchain = 'base',
    project = 'sonne_finance',
    version = '1',
    decoded_project = 'sonne_finance',
    sources = config_sources
  )
}}
