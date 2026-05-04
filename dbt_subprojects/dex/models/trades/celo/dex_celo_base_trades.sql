{{ config(
    schema = 'dex_celo'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set base_models = [
    ref('uniswap_v3_celo_base_trades'),
    ref('mento_v1_celo_base_trades'),
    ref('mento_v2_celo_base_trades'),
    ref('curvefi_celo_base_trades'),
    ref('sushiswap_celo_base_trades'),
    ref('ubeswap_celo_base_trades'),
    ref('carbonhood_celo_base_trades'),
    ref('carbon_defi_celo_base_trades'),
    ref('gooddollar_reserve_celo_base_trades'),
    ref('uniswap_v4_celo_base_trades'),
    ref('velodrome_celo_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'celo',
    base_models = base_models
) }}
