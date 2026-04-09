{{ config(
    schema = 'dex_monad'
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
    ref('kuru_monad_base_trades')
    , ref('pinot_v2_monad_base_trades')
    , ref('pinot_v3_monad_base_trades')
    , ref('uniswap_v2_monad_base_trades')
    , ref('uniswap_v3_monad_base_trades')
    , ref('trader_joe_v2_2_monad_base_trades')
    , ref('uniswap_v4_monad_base_trades')
    , ref('pancakeswap_v2_monad_base_trades')
    , ref('pancakeswap_v3_monad_base_trades')
    , ref('balancer_v3_monad_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'monad',
    base_models = base_models
) }}
