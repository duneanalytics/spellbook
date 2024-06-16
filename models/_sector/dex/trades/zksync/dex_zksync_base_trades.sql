{{ config(
    schema = 'dex_zksync'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('maverick_zksync_base_trades')
    , ref('pancakeswap_v2_zksync_base_trades')
    , ref('pancakeswap_v3_zksync_base_trades')
    , ref('syncswap_v1_zksync_base_trades')
    , ref('syncswap_v2_zksync_base_trades')
    , ref('uniswap_v3_zksync_base_trades')
    , ref('mute_zksync_base_trades')
    , ref('spacefi_v1_zksync_base_trades')
    , ref('derpdex_v1_zksync_base_trades')
    , ref('ezkalibur_v2_zksync_base_trades')
    , ref('wagmi_v1_zksync_base_trades')
    , ref('zkswap_finance_zksync_base_trades')
    , ref('gemswap_zksync_base_trades')
    , ref('vesync_v1_zksync_base_trades')
    , ref('dracula_finance_zksync_base_trades')
    , ref('iziswap_v1_zksync_base_trades')
    , ref('iziswap_v2_zksync_base_trades')
    , ref('velocore_v0_zksync_base_trades')
    , ref('velocore_v1_zksync_base_trades')
    , ref('velocore_v2_zksync_base_trades')
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
        , blockchain = 'zksync'
        , columns = ['from', 'to', 'index']
    )
}}
