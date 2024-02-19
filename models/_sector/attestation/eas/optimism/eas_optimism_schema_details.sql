{{
  config(
    schema = 'eas_optimism',
    alias = 'schema_details',
    materialized = 'view',
    unique_key = ['schema_uid', 'ordinality']
  )
}}

{{
  eas_schema_details(
    blockchain = 'optimism',
    project = 'eas',
    version = '1'
  )
}}
