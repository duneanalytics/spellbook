{{
  config(
    schema = 'manifest_solana'
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
    project = 'manifest'
    , project_main_id = 'MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms'
    , project_start_date = '2025-07-31'
    , stg_raw_swaps_model = ref('manifest_solana_stg_raw_swaps')
    , token_bought_offset = 2
    , token_sold_offset = 1
) }}
