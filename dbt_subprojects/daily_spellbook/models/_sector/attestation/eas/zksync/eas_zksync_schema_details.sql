{{
  config(
    schema = 'eas_zksync',
    alias = 'schema_details',
    materialized = 'view',
    unique_key = ['schema_uid', 'ordinality']
  )
}}

{{
  eas_schema_details(
    blockchain = 'zksync',
    project = 'eas',
    version = '1'
  )
}}
