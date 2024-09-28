{{
  config(
    schema = 'eas_celo',
    alias = 'schema_details',
    materialized = 'view',
    unique_key = ['schema_uid', 'ordinality']
  )
}}

{{
  eas_schema_details(
    blockchain = 'celo',
    project = 'eas',
    version = '1'
  )
}}
