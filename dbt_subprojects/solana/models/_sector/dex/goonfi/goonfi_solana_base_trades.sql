{{
  config(
    schema = 'goonfi_solana'
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
    project = 'goonfi'
    , project_main_id = 'goonERTdGsjnkZqWuVjs73BZ3Pb9qoCUdBUL17BnS5j'
    , project_start_date = '2025-05-26'
    , stg_raw_swaps_model = ref('goonfi_solana_stg_raw_swaps')
    , token_bought_offset = 2
    , token_sold_offset = 1
) }}
