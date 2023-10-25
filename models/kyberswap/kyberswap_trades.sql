{{ config(
        alias = 'trades',
        post_hook='{{ expose_spells(\'["avalanche_c","optimism","ethereum","arbitrum","bnb","polygon"]\',
                                "project",
                                "kyberswap",
                                \'["zhongyiio", "hosuke", "ppclunghe", "gregshestakovlido", "nhd98z"]\') }}'
        )
}}

{% set kyber_models = [
    ref('kyberswap_avalanche_c_trades')
    ,ref('kyberswap_optimism_trades')
    ,ref('kyberswap_ethereum_trades')
    ,ref('kyberswap_arbitrum_trades')
    ,ref('kyberswap_bnb_trades')
    ,ref('kyberswap_polygon_trades')
] %}


SELECT *
FROM (
    {% for k_model in kyber_models %}
    SELECT
        blockchain,
        project,
        version,
        block_month,
        block_date,
        block_time,
        token_bought_symbol,
        token_sold_symbol,
        token_pair,
        token_bought_amount,
        token_sold_amount,
        token_bought_amount_raw,
        token_sold_amount_raw,
        amount_usd,
        token_bought_address,
        token_sold_address,
        taker,
        maker,
        project_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        evt_index
    FROM {{ k_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
