{{
  config(
    schema = 'realt_rmm_v2_gnosis',
    alias = 'base_supply',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  lending_aave_v3_compatible_supply(
    blockchain = 'gnosis',
    project = 'realt_rmm',
    version = '2',
    project_decoded_as = 'real_rmm_v2'
  )
}}
