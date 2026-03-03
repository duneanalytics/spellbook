{{
  config(
    schema = 'justlend_tron',
    alias = 'base_supply',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{% set config_sources = [
  {'contract': 'jusdt'},
  {'contract': 'jusdd'},
  {'contract': 'cether'}
] %}

{{
  lending_compound_v2_compatible_supply(
    blockchain = 'tron',
    project = 'justlend',
    version = '1',
    decoded_project = 'justlend',
    sources = config_sources
  )
}}
