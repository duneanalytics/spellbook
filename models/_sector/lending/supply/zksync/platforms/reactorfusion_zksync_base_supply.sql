{{
  config(
    schema = 'reactorfusion_zksync',
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
    {'contract': 'rfUSDC'},
    {'contract': 'rfUSDT'},
    {'contract': 'rfWBTC'},
    {'contract': 'rfETH'},
  ]
%}

{{
  lending_compound_v2_compatible_supply(
    blockchain = 'zksync',
    project = 'reactorfusion',
    version = '1',
    sources = config_sources
  )
}}
