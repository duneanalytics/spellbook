{{
  config(
    schema = 'solfi_v2_solana'
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
    program_id = 'SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF'
    , discriminator_filter = "BYTEARRAY_SUBSTRING(data, 1, 1) IN (0x07, 0x0e)"
    , project_start_date = '2026-04-01'
    , pool_id_expression = "account_arguments[2]"
) }}
