{{
  config(
    schema = 'goonfi_v2_solana'
    , alias = 'stg_raw_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
    , tags = ['prod_exclude']
  )
}}

{{ solana_amm_stg_raw_swaps(
    program_id = 'goonuddtQRrWqqn5nFyczVKaie28f3kDkHWkHtURSLE'
    , discriminator_filter = "BYTEARRAY_SUBSTRING(data, 1, 1) = 0x01"
    , project_start_date = '2025-12-12'
    , pool_id_expression = "account_arguments[2]"
) }}
