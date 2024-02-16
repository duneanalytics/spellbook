{{
  config(
    schema = 'strike_ethereum',
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
    {'contract': 'SErc20Delegate'},
    {'contract': 'SEther'},
  ]
%}

{{
  lending_compound_v2_compatible_supply(
    blockchain = 'ethereum',
    project = 'strike',
    version = '1',
    decoded_project = 'strike_lending',
    sources = config_sources
  )
}}
