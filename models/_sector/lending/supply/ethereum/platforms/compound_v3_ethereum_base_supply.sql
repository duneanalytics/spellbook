{{
  config(
    schema = 'compound_v3_ethereum',
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
    {'contract': 'Comet'},
    {'contract': 'cWETHv3'},
  ]
%}

{{
  lending_compound_v3_compatible_supply(
    blockchain = 'ethereum',
    project = 'compound',
    version = '3',
    sources = config_sources
  )
}}
