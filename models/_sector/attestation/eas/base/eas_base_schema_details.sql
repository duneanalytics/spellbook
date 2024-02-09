{{
  config(
    schema = 'eas_base',
    alias = 'schema_details',
    materialized = 'view',
    unique_key = ['schema_uid', 'ordinality']
  )
}}

{{
  eas_schema_details(
    blockchain = 'base',
    project = 'eas',
    version = '1'
  )
}}
