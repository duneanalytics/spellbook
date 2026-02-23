{{
  config(
    schema = 'whalestreet_solana'
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
    program_id = 'FW6zUqn4iKRaeopwwhwsquTY6ABWLLgjxtrC3VPnaWBf'
    , discriminator_filter = "BYTEARRAY_SUBSTRING(data, 1, 8) = 0xf8c69e91e17587c8"
    , project_start_date = '2025-11-20'
    , pool_id_expression = "CAST(NULL AS VARCHAR)"
) }}
