{{
  config(
    schema = 'eas_arbitrum',
    alias = 'schema_details',
    materialized = 'view',
    unique_key = ['schema_uid', 'ordinality']
  )
}}

{{
  eas_schema_details(
    blockchain = 'arbitrum',
    project = 'eas',
    version = '1'
  )
}}
