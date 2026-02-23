{{
  config(
    schema = 'bisonfi_solana'
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
    program_id = 'BiSoNHVpsVZW2F7rx2eQ59yQwKxzU5NvBcmKshCSUypi'
    , discriminator_filter = "(BYTEARRAY_SUBSTRING(data, 1, 1) IN (0x02, 0x07)) AND cardinality(account_arguments) = 9"
    , project_start_date = '2025-11-05'
    , pool_id_expression = "account_arguments[2]"
) }}
