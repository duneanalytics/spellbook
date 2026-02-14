{{
  config(
    schema = 'goonfi_solana'
    , alias = 'v2_base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{{ solana_amm_base_trades(
    project = 'goonfi'
    , project_main_id = 'goonuddtQRrWqqn5nFyczVKaie28f3kDkHWkHtURSLE'
    , project_start_date = '2025-12-12'
    , stg_raw_swaps_model = ref('goonfi_v2_solana_stg_raw_swaps')
    , token_bought_offset = 2
    , token_sold_offset = 1
    , version = 2
    , version_name = 'v2'
) }}
