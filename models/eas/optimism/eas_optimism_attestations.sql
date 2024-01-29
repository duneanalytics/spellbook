{{
  config(
    schema = 'eas_optimism',
    alias = 'attestations',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  eas_attestations(
    blockchain = 'optimism',
    project = 'eas',
    version = '1',
    decoded_project_name = 'attestationstation_v1_optimism'
  )
}}
