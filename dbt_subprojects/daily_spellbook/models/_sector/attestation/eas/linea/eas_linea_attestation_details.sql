{{
  config(
    schema = 'eas_linea',
    alias = 'attestation_details',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['schema_uid', 'attestation_uid', 'ordinality'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  eas_attestation_details(
    blockchain = 'linea',
    project = 'eas',
    version = '1'
  )
}}
