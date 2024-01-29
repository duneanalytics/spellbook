{{
  config(
    schema = 'eas_ethereum',
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
    blockchain = 'ethereum',
    project = 'eas',
    version = '1',
    decoded_project_name = 'ethereum_attestation_service'
  )
}}
