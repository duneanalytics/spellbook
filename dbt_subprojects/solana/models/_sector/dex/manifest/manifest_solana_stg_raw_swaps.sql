{{
  config(
    schema = 'manifest_solana'
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
    program_id = 'MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms'
    , discriminator_filter = "BYTEARRAY_SUBSTRING(data, 1, 1) IN (0x0d, 0x04)"
    , project_start_date = '2025-07-31'
    , pool_id_expression = "CAST(NULL AS VARCHAR)"
) }}
