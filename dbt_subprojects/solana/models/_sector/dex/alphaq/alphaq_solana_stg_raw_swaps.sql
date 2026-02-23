{{
  config(
    schema = 'alphaq_solana'
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
    program_id = 'ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA'
    , discriminator_filter = "BYTEARRAY_SUBSTRING(data, 1, 1) = 0x0c"
    , project_start_date = '2025-08-29'
    , pool_id_expression = "CAST(NULL AS VARCHAR)"
) }}
