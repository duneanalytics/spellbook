{{
  config(
    schema = 'aquifer_v2_solana'
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
    project = 'aquifer'
    , project_main_id = 'AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45'
    , project_start_date = '2026-01-30'
    , stg_raw_swaps_model = ref('aquifer_v2_solana_stg_raw_swaps')
    , token_bought_offset = 1
    , token_sold_offset = 2
    , version = 2
    , version_name = 'v2'
) }}
