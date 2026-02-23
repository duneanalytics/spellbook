{{
  config(
    schema = 'byreal_solana'
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
    program_id = 'REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2'
    , discriminator_filter = "BYTEARRAY_SUBSTRING(data, 1, 8) IN (0x2b04ed0b1ac91e62, 0xf8c69e91e17587c8)"
    , project_start_date = '2025-06-26'
    , pool_id_expression = "account_arguments[3]"
) }}
