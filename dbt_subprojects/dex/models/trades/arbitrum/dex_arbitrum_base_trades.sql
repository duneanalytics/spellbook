{{ config(
    schema = 'dex_arbitrum'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('uniswap_v3_arbitrum_base_trades')
    , ref('camelot_v2_arbitrum_base_trades')
    , ref('camelot_v3_arbitrum_base_trades')
    , ref('airswap_arbitrum_base_trades')
    , ref('sushiswap_v1_arbitrum_base_trades')
    , ref('sushiswap_v2_arbitrum_base_trades')
    , ref('arbswap_arbitrum_base_trades')
    , ref('trader_joe_v2_arbitrum_base_trades')
    , ref('trader_joe_v2_1_arbitrum_base_trades')
    , ref('trader_joe_v2_2_arbitrum_base_trades')
    , ref('pancakeswap_v2_arbitrum_base_trades')
    , ref('pancakeswap_v3_arbitrum_base_trades')
    , ref('balancer_v2_arbitrum_base_trades')
    , ref('dodo_arbitrum_base_trades')
    , ref('gmx_arbitrum_base_trades')
    , ref('integral_arbitrum_base_trades')
    , ref('kyberswap_arbitrum_base_trades')
    , ref('clipper_arbitrum_base_trades')
    , ref('ramses_arbitrum_base_trades')
    , ref('xchange_arbitrum_base_trades')
    , ref('fraxswap_arbitrum_base_trades')
    , ref('chronos_arbitrum_base_trades')
    , ref('zyberswap_arbitrum_base_trades')
    , ref('solidlizard_arbitrum_base_trades')
    , ref('rubicon_arbitrum_base_trades')
    , ref('apeswap_arbitrum_base_trades')
    , ref('oasisswap_arbitrum_base_trades')
    , ref('smardex_arbitrum_base_trades')
    , ref('swaap_v2_arbitrum_base_trades')
    , ref('woofi_arbitrum_base_trades')
    , ref('zigzag_arbitrum_base_trades')
    , ref('gridex_arbitrum_base_trades')
    , ref('sterling_finance_arbitrum_base_trades')
    , ref('sharkyswap_arbitrum_base_trades')
    , ref('uniswap_v2_arbitrum_base_trades')
    , ref('auragi_arbitrum_base_trades')
    , ref('wombat_exchange_arbitrum_base_trades')
    , ref('solidly_v3_arbitrum_base_trades')
    , ref('dackieswap_v3_arbitrum_base_trades')
    , ref('dackieswap_v2_arbitrum_base_trades')
    , ref('maverick_v2_arbitrum_base_trades')
    , ref('valantis_hot_arbitrum_base_trades')
] %}

WITH base_union AS (
    SELECT *
    FROM (
        {% for base_model in base_models %}
        SELECT
            blockchain
            , project
            , version
            , block_month
            , block_date
            , block_time
            , block_number
            , token_bought_amount_raw
            , token_sold_amount_raw
            , token_bought_address
            , token_sold_address
            , taker
            , maker
            , project_contract_address
            , tx_hash
            , evt_index
        FROM
            {{ base_model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

{{
    add_tx_columns(
        model_cte = 'base_union'
        , blockchain = 'arbitrum'
        , columns = ['from', 'to', 'index']
    )
}}
