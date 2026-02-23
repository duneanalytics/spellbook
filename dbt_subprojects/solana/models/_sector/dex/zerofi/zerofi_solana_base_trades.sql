{{
  config(
    schema = 'zerofi_solana'
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
    project = 'zerofi'
    , project_main_id = 'ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY'
    , project_start_date = '2024-12-12'
    , stg_raw_swaps_model = ref('zerofi_solana_stg_raw_swaps')
    , token_bought_offset = 2
    , token_sold_offset = 1
) }}
