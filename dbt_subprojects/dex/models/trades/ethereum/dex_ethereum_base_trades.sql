{{ config(
    schema = 'dex_ethereum'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('defiswap_ethereum_base_trades')
    , ref('uniswap_v1_ethereum_base_trades')
    , ref('uniswap_v2_ethereum_base_trades')
    , ref('uniswap_v3_ethereum_base_trades')
    , ref('apeswap_ethereum_base_trades')
    , ref('carbon_defi_ethereum_base_trades')
    , ref('airswap_ethereum_base_trades')
    , ref('sushiswap_v1_ethereum_base_trades')
    , ref('sushiswap_v2_ethereum_base_trades')
    , ref('pancakeswap_v2_ethereum_base_trades')
    , ref('pancakeswap_v3_ethereum_base_trades')
    , ref('shibaswap_v1_ethereum_base_trades')
    , ref('balancer_v1_ethereum_base_trades')
    , ref('balancer_v2_ethereum_base_trades')
    , ref('fraxswap_ethereum_base_trades')
    , ref('bancor_ethereum_base_trades')
    , ref('verse_dex_ethereum_base_trades')
    , ref('swapr_ethereum_base_trades')
    , ref('mauve_ethereum_base_trades')
    , ref('dfx_ethereum_base_trades')
    , ref('dodo_ethereum_base_trades')
    , ref('integral_ethereum_base_trades')
    , ref('maverick_ethereum_base_trades')
    , ref('maverick_v2_ethereum_base_trades')
    , ref('kyberswap_ethereum_base_trades')
    , ref('clipper_ethereum_base_trades')
    , ref('mstable_ethereum_base_trades')
    , ref('xchange_ethereum_base_trades')
    , ref('curve_ethereum_base_trades')
    , ref('solidly_v3_ethereum_base_trades')
    , ref('swaap_v2_ethereum_base_trades')
    , ref('valantis_hot_ethereum_base_trades')
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
        , blockchain = 'ethereum'
        , columns = ['from', 'to', 'index']
    )
}}
