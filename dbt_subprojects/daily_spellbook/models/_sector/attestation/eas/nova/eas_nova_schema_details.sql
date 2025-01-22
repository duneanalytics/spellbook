{{
  config(
    schema = 'eas_nova',
    alias = 'schema_details',
    materialized = 'view',
    unique_key = ['schema_uid', 'ordinality']
  )
}}

{{
  eas_schema_details(
    blockchain = 'nova',
    project = 'eas',
    version = '1'
  )
}}
