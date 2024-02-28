{{
  config(
    schema = 'eas_polygon',
    alias = 'attestations',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['schema_uid', 'attestation_uid'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  eas_attestations(
    blockchain = 'polygon',
    project = 'eas',
    version = '1',
    decoded_project_name = 'polygon_eas',
    schema_column_name = 'schemaUID'
  )
}}
