{{ config(
    schema = 'dex_bnb'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}


-- (blockchain, project, project_version, model)
{% set base_models = [
    ref('uniswap_v3_bnb_base_trades')
    , ref('apeswap_bnb_base_trades')
    , ref('airswap_bnb_base_trades')
    , ref('sushiswap_v1_bnb_base_trades')
    , ref('sushiswap_v2_bnb_base_trades')
    , ref('fraxswap_bnb_base_trades')
    , ref('trader_joe_v2_bnb_base_trades')
    , ref('trader_joe_v2_1_bnb_base_trades')
    , ref('pancakeswap_v2_bnb_base_trades')
    , ref('pancakeswap_v3_bnb_base_trades')
    , ref('biswap_v2_bnb_base_trades')
    , ref('biswap_v3_bnb_base_trades')
    , ref('babyswap_bnb_base_trades')
    , ref('mdex_bnb_base_trades')
    , ref('wombat_bnb_base_trades')
    , ref('dodo_bnb_base_trades')
    , ref('iziswap_bnb_base_trades')
    , ref('maverick_bnb_base_trades')
    , ref('nomiswap_bnb_base_trades')
    , ref('kyberswap_bnb_base_trades')
    , ref('xchange_bnb_base_trades')
    , ref('thena_bnb_base_trades')
    , ref('ellipsis_finance_bnb_base_trades')
    , ref('onepunchswap_bnb_base_trades')
    , ref('woofi_bnb_base_trades')
    , ref('hashflow_bnb_base_trades')
    , ref('uniswap_v2_bnb_base_trades')
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
        , blockchain = 'bnb'
        , columns = ['from', 'to', 'index']
    )
}}