{{
  config(
    schema = 'obric_solana'
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
    program_id = 'obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y'
    , discriminator_filter = "BYTEARRAY_SUBSTRING(data, 1, 8) IN (0xf8c69e91e17587c8, 0x414b3f4ceb5b5b88)"
    , project_start_date = '2024-06-04'
    , pool_id_expression = "account_arguments[1]"
) }}
