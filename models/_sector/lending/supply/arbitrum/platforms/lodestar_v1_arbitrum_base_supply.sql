{{
  config(
    schema = 'lodestar_v1_arbitrum',
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
    {'contract': 'lETH_V3'},
    {'contract': 'lWBTC_V3'},
    {'contract': 'lUSDC_V3'},
    {'contract': 'lUSDT_V3'},
    {'contract': 'lMAGIC_V3'},
    {'contract': 'lARB_V3'},
    {'contract': 'lDAI_V3'},
    {'contract': 'lFRAX_V3'},
    {'contract': 'lDPX_V3'},
    {'contract': 'lwstETH'},
    {'contract': 'lGMX'},
  ]
%}

{{
  lending_compound_v2_compatible_supply(
    blockchain = 'arbitrum',
    project = 'lodestar',
    version = '1',
    decoded_project = 'lodestar',
    sources = config_sources
  )
}}
