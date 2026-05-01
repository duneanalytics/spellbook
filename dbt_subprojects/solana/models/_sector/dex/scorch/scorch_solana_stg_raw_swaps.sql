{{
  config(
    schema = 'scorch_solana'
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
    program_id = 'SCoRcH8c2dpjvcJD6FiPbCSQyQgu3PcUAWj2Xxx3mqn'
    , discriminator_filter = "(BYTEARRAY_SUBSTRING(data, 1, 1) IN (0x02, 0x01)) AND cardinality(account_arguments) > 10"
    , project_start_date = '2025-11-28'
    , pool_id_expression = "CAST(NULL AS VARCHAR)"
) }}
