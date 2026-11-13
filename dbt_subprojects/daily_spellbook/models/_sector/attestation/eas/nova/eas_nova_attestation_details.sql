{{
  config(
    schema = 'eas_nova',
    alias = 'attestation_details',
    materialized = 'table',
    tags = ['static'],
    file_format = 'delta',
  )
}}

{{
  eas_attestation_details(
    blockchain = 'nova',
    project = 'eas',
    version = '1'
  )
}}
