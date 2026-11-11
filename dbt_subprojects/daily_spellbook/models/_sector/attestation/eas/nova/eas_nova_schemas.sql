{{
  config(
    schema = 'eas_nova',
    alias = 'schemas',
    materialized = 'table',
    tags = ['static'],
    file_format = 'delta',
  )
}}

{{
  eas_schemas(
    blockchain = 'nova',
    project = 'eas',
    version = '1'
  )
}}
