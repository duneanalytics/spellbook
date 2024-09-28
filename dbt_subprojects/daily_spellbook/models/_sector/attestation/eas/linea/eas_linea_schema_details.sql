{{
  config(
    schema = 'eas_linea',
    alias = 'schema_details',
    materialized = 'view',
    unique_key = ['schema_uid', 'ordinality']
  )
}}

{{
  eas_schema_details(
    blockchain = 'linea',
    project = 'eas',
    version = '1'
  )
}}
