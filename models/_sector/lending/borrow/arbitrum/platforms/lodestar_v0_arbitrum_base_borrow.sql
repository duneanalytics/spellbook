{{
  config(
    schema = 'lodestar_v0_arbitrum',
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
    {'contract': 'lETH_V2'},
    {'contract': 'lWBTC_V2'},
    {'contract': 'lUSDC_V2'},
    {'contract': 'lUSDT_V2'},
    {'contract': 'lMAGIC_V2'},
    {'contract': 'lARB_V2'},
    {'contract': 'lDAI_V2'},
    {'contract': 'lFRAX_V2'},
    {'contract': 'lDPX_V2'},
  ]
%}

{{
  lending_compound_v2_compatible_borrow(
    blockchain = 'arbitrum',
    project = 'lodestar',
    version = '0',
    decoded_project = 'lodestar',
    sources = config_sources
  )
}}
