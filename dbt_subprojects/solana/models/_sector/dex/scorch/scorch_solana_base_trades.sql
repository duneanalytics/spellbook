{{
  config(
    schema = 'scorch_solana'
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
    project = 'scorch'
    , project_main_id = 'SCoRcH8c2dpjvcJD6FiPbCSQyQgu3PcUAWj2Xxx3mqn'
    , project_start_date = '2025-11-28'
    , stg_raw_swaps_model = ref('scorch_solana_stg_raw_swaps')
    , token_bought_offset = 3
    , token_sold_offset = 2
) }}
