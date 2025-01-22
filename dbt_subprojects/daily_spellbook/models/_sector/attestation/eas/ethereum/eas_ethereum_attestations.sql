{{
  config(
    schema = 'eas_ethereum',
    alias = 'attestations',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['schema_uid', 'attestation_uid']
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
