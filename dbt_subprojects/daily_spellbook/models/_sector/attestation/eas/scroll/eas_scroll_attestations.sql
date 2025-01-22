{{
  config(
    schema = 'eas_scroll',
    alias = 'attestations',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['schema_uid', 'attestation_uid']
  )
}}

{{
  eas_attestations(
    blockchain = 'scroll',
    project = 'eas',
    version = '1',
    schema_column_name = 'schemaUID'
  )
}}
