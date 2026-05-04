{{
  config(
    schema = 'sunswap_v2_tron',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  uniswap_compatible_v2_trades(
    blockchain = 'tron',
    project = 'sunswap',
    version = '2',
    Pair_evt_Swap = ref('sunswap_v2_tron_swap_events'),
    Factory_evt_PairCreated = source('sunswap_v2_tron', 'sunswapv2factory_evt_paircreated')
  )
}}
