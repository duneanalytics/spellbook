{{
  config(
    schema = 'eas_nova',
    alias = 'attestations',
    materialized = 'table',
    tags = ['static'],
    file_format = 'delta',
  )
}}

{{
  eas_attestations(
    blockchain = 'nova',
    project = 'eas',
    version = '1',
    schema_column_name = 'schemaUID'
  )
}}
