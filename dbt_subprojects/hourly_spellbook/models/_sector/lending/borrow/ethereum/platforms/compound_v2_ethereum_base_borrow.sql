{{
  config(
    schema = 'compound_v2_ethereum',
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
    {'contract': 'cErc20'},
    {'contract': 'cEther'},
  ]
%}

{{
  lending_compound_v2_compatible_borrow(
    blockchain = 'ethereum',
    project = 'compound',
    version = '2',
    sources = config_sources
  )
}}
