{{
  config(
    schema = 'spark_ethereum',
    alias = 'base_flashloans',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  lending_aave_v3_compatible_flashloans(
    blockchain = 'ethereum',
    project = 'spark',
    version = '1',
    project_decoded_as = 'spark_protocol'
  )
}}
