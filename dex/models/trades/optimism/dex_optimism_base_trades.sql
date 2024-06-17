{{ config(
    schema = 'dex_optimism'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('uniswap_v3_optimism_base_trades')
    , ref('woofi_optimism_base_trades')
    , ref('mummy_finance_optimism_base_trades')
    , ref('sushiswap_v1_optimism_base_trades')
    , ref('sushiswap_v2_optimism_base_trades')
    , ref('hashflow_optimism_base_trades')
    , ref('zipswap_optimism_base_trades')
    , ref('balancer_v2_optimism_base_trades')
    , ref('wardenswap_optimism_base_trades')
    , ref('dodo_optimism_base_trades')
    , ref('kyberswap_optimism_base_trades')
    , ref('clipper_optimism_base_trades')
    , ref('opx_finance_optimism_base_trades')
    , ref('velodrome_optimism_base_trades')
    , ref('synthetix_optimism_base_trades')
    , ref('openxswap_optimism_base_trades')
    , ref('openocean_optimism_base_trades')
    , ref('chainhop_optimism_base_trades')
    , ref('curvefi_optimism_base_trades')
    , ref('rubicon_optimism_base_trades')
    , ref('gridex_optimism_base_trades')
    , ref('solidly_v3_optimism_base_trades')
    , ref('dackieswap_v2_optimism_base_trades')
    , ref('dackieswap_v3_optimism_base_trades')
    , ref('wombat_exchange_optimism_base_trades')
    , ref('uniswap_v2_optimism_base_trades')
    , ref('elk_finance_optimism_base_trades')
    , ref('fraxswap_optimism_base_trades')
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
        , blockchain = 'optimism'
        , columns = ['from', 'to', 'index']
    )
}}