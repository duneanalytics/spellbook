{{
  config(
    schema = 'deriverse_solana'
    , alias = 'stg_raw_swaps_b_to_a'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{{ solana_amm_stg_raw_swaps(
    program_id = 'DRVSpZ2YUYYKgZP8XtLhAGtT1zYSCKzeHfb4DgRnrgqD'
    , discriminator_filter = "BYTEARRAY_SUBSTRING(data, 1, 2) = 0x1a01 AND cardinality(account_arguments) >= 14"
    , project_start_date = '2026-11-01'
    , pool_id_expression = "account_arguments[6]"
) }}
