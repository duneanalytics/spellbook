{{ config(
    schema = 'dex_hemi'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , unique_key = ['tx_hash', 'evt_index']
    
    )
}}

{% set base_models = [
    ref('oku_v3_hemi_base_trades')
    , ref('izumi_finance_hemi_base_trades')
    , ref('sushiswap_v2_hemi_base_trades')
    , ref('sushiswap_v3_hemi_base_trades')
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
        , blockchain = 'hemi'
        , columns = ['from', 'to', 'index']
    )
}}
