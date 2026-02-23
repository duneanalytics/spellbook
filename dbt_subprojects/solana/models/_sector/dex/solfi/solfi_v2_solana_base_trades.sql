{{
  config(
    schema = 'solfi_v2_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
    , tags = ['prod_exclude']
  )
}}

{{ solana_amm_base_trades(
    project = 'solfi'
    , project_main_id = 'SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF'
    , project_start_date = '2025-08-07'
    , stg_raw_swaps_model = ref('solfi_v2_solana_stg_raw_swaps')
    , token_bought_offset = 2
    , token_sold_offset = 1
    , version = 2
    , version_name = 'v2'
) }}
