{{
  config(
    schema = 'humidifi_solana'
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
    program_id = '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp'
    , discriminator_filter = "cardinality(account_arguments) = 9"
    , project_start_date = '2025-06-13'
    , pool_id_expression = "account_arguments[2]"
) }}
