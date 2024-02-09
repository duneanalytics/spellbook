{{
  config(
    schema = 'eas_ethereum',
    alias = 'schemas',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['schema_uid'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  eas_schemas(
    blockchain = 'ethereum',
    project = 'eas',
    version = '1',
    decoded_project_name = 'ethereum_attestation_service_ethereum_schema'
  )
}}
