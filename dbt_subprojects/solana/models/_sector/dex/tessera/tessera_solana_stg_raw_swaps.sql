{{
  config(
    schema = 'tessera_solana'
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
    program_id = 'TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH'
    , discriminator_filter = "BYTEARRAY_SUBSTRING(data, 1, 1) = 0x10"
    , project_start_date = '2025-06-12'
    , pool_id_expression = "account_arguments[2]"
) }}
