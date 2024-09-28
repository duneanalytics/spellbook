{{
  config(
    schema = 'compound_v3_polygon',
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
    {'contract': 'cUSDCv3Polygon'},
  ]
%}

{{
  lending_compound_v3_compatible_supply(
    blockchain = 'polygon',
    project = 'compound',
    version = '3',
    sources = config_sources
  )
}}
