{{
  config(
    schema = 'eas_linea',
    alias = 'attestations',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['schema_uid', 'attestation_uid']
  )
}}

{{
  eas_attestations(
    blockchain = 'linea',
    project = 'eas',
    version = '1',
    decoded_project_name = 'attestationstation_v1',
    schema_column_name = 'schemaUID'
  )
}}
