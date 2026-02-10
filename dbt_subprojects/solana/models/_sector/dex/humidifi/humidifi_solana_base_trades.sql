{{
  config(
    schema = 'humidifi_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{{ solana_amm_base_trades(
    project = 'humidifi'
    , project_main_id = '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp'
    , project_start_date = '2025-06-13'
    , stg_raw_swaps_model = ref('humidifi_solana_stg_raw_swaps')
    , token_bought_offset = 1
    , token_sold_offset = 2
) }}
