{{
  config(
    schema = 'deriverse_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

-- The second data byte sets the swap direction: 0x00 transfers the sold token first, 0x01 transfers the bought token first.
SELECT * FROM (
{{ solana_amm_base_trades(
    project = 'deriverse'
    , project_main_id = 'DRVSpZ2YUYYKgZP8XtLhAGtT1zYSCKzeHfb4DgRnrgqD'
    , project_start_date = '2026-11-01'
    , stg_raw_swaps_model = ref('deriverse_solana_stg_raw_swaps_a_to_b')
    , token_bought_offset = 2
    , token_sold_offset = 1
) }}
)

UNION ALL

SELECT * FROM (
{{ solana_amm_base_trades(
    project = 'deriverse'
    , project_main_id = 'DRVSpZ2YUYYKgZP8XtLhAGtT1zYSCKzeHfb4DgRnrgqD'
    , project_start_date = '2026-11-01'
    , stg_raw_swaps_model = ref('deriverse_solana_stg_raw_swaps_b_to_a')
    , token_bought_offset = 1
    , token_sold_offset = 2
) }}
)
