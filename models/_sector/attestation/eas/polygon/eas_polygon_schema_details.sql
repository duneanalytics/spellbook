{{
  config(
    schema = 'eas_polygon',
    alias = 'schema_details',
    materialized = 'view',
    unique_key = ['schema_uid', 'ordinality_id']
  )
}}

{{
  eas_schema_details(
    blockchain = 'polygon',
    project = 'eas',
    version = '1'
  )
}}
