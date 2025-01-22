{{
  config(
    schema = 'moonwell_base',
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
    {'contract': 'mcbETH'},
    {'contract': 'mDAI'},
    {'contract': 'mUSDC'},
    {'contract': 'mUSDCnative'},
    {'contract': 'mWETH'},
    {'contract': 'mwstETH'},
    {'contract': 'mrETH'},
  ]
%}

{{
  lending_compound_v2_compatible_borrow(
    blockchain = 'base',
    project = 'moonwell',
    version = '1',
    decoded_project = 'moonwell',
    sources = config_sources
  )
}}
 