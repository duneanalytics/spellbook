{{ config(
    schema = 'dex_all_chains'
    , alias = 'automated_base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
      ref('uniswap_v2_all_chains_automated_base_trades')
    , ref('uniswap_v3_all_chains_automated_base_trades')
] %}

SELECT *
FROM (
    {% for base_model in base_models %}
    SELECT
        blockchain
        , version
        , dex_type
        , factory_address
        , factory_topic0
        , factory_info
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
        , pool_topic0
        , tx_hash
        , evt_index
        , tx_from
        , tx_to
        , tx_index
    FROM
        {{ base_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
