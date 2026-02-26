{{
  config(
    schema = 'aquifer_solana'
    , alias = 'stg_raw_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{{ solana_amm_stg_raw_swaps(
    program_id = 'AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45'
    , discriminator_filter = "BYTEARRAY_SUBSTRING(data, 1, 1) = 0x01"
    , project_start_date = '2025-06-26'
    , pool_id_expression = "CAST(NULL AS VARCHAR)"
) }}
