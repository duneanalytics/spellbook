{{
  config(
    schema = 'benqi_avalanche_c',
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
    {'contract': 'QiErc20Delegate'},
    {'contract': 'QiAvax'},
  ]
%}

{{
  lending_compound_v2_compatible_supply(
    blockchain = 'avalanche_c',
    project = 'benqi',
    version = '1',
    decoded_project = 'benqi_finance',
    sources = config_sources
  )
}}
