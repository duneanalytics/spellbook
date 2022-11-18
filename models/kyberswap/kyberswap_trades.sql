{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "binance_smart_chain", "ethereum", "optimism", "polygon"]\',
                                "project",
                                "kyberswap",
                                \'["zhongyiio", "hosuke"]\') }}'
        )
}}

{% set kyber_models = [
'kyberswap_arbitrum_trades',
'kyberswap_avalanche_c_trades',
'kyberswap_bsc_trades',
'kyberswap_ethereum_trades',
'kyberswap_optimism_trades',
'kyberswap_polygon_trades'
] %}


SELECT *
FROM (
    {% for dex_model in kyber_models %}
    SELECT
        blockchain,
        project,
        version,
        category,
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
        trace_address,
        evt_index
    FROM {{ ref(dex_model) }}
    {% if not loop.last %}
    UNION
    {% endif %}
    {% endfor %}
)
;