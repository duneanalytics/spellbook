{{
  config(
    schema = 'obric_solana'
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
    project = 'obric'
    , project_main_id = 'obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y'
    , project_start_date = '2024-06-04'
    , stg_raw_swaps_model = ref('obric_solana_stg_raw_swaps')
    , token_bought_offset = 2
    , token_sold_offset = 1
) }}
