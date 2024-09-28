{{
  config(
    schema = 'eas_ethereum',
    alias = 'schema_details',
    materialized = 'view',
    unique_key = ['schema_uid', 'ordinality']
  )
}}

{{
  eas_schema_details(
    blockchain = 'ethereum',
    project = 'eas',
    version = '1'
  )
}}
