{{
  config(
    schema = 'eas_scroll',
    alias = 'schema_details',
    materialized = 'view',
    unique_key = ['schema_uid', 'ordinality']
  )
}}

{{
  eas_schema_details(
    blockchain = 'scroll',
    project = 'eas',
    version = '1'
  )
}}
