{{ config(
    schema = 'dex_bnb'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}


-- (blockchain, project, project_version, model)
{% set base_models = [
    ref('uniswap_v4_bnb_base_trades')
    , ref('uniswap_v3_bnb_base_trades')
    , ref('uniswap_v2_bnb_base_trades')
    , ref('apeswap_bnb_base_trades')
    , ref('airswap_bnb_base_trades')
    , ref('sushiswap_v1_bnb_base_trades')
    , ref('sushiswap_v2_bnb_base_trades')
    , ref('fraxswap_bnb_base_trades')
    , ref('trader_joe_v2_bnb_base_trades')
    , ref('trader_joe_v2_1_bnb_base_trades')
    , ref('pancakeswap_v2_bnb_base_trades')
    , ref('pancakeswap_v3_bnb_base_trades')
    , ref('pancakeswap_infinity_bnb_base_trades')
    , ref('biswap_v2_bnb_base_trades')
    , ref('biswap_v3_bnb_base_trades')
    , ref('babyswap_bnb_base_trades')
    , ref('mdex_bnb_base_trades')
    , ref('wombat_bnb_base_trades')
    , ref('dodo_bnb_base_trades')
    , ref('izumi_finance_bnb_base_trades')
    , ref('maverick_bnb_base_trades')
    , ref('maverick_v2_bnb_base_trades')
    , ref('nomiswap_bnb_base_trades')
    , ref('kyberswap_bnb_base_trades')
    , ref('xchange_bnb_base_trades')
    , ref('thena_bnb_base_trades')
    , ref('ellipsis_finance_bnb_base_trades')
    , ref('onepunchswap_bnb_base_trades')
    , ref('woofi_bnb_base_trades')
    , ref('hashflow_bnb_base_trades')
    , ref('swaap_v2_bnb_base_trades')
    , ref('hyperjump_bnb_base_trades')
    , ref('native_bnb_base_trades')
    , ref('eulerswap_bnb_base_trades')
    , ref('zeroex_bnb_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'bnb',
    base_models = base_models
) }}
