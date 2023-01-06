{{ config(
        alias='trades',
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "optimism", "polygon"]\',
                        "project",
                        "wardenswap",
                        \'["codingsh", "jeff-dude", "hosuke"]\') }}'
        )
}}

{% set wardenswap_models = [
ref('wardenswap_bnb_trades')
,ref('wardenswap_ethereum_trades')
,ref('wardenswap_arbitrum_trades')
,ref('wardenswap_avalanche_c_trades')
,ref('wardenswap_optimism_trades')
,ref('wardenswap_polygon_trades')
] %}

SELECT *
FROM (
    {% for dex_model in wardenswap_models %}
    SELECT
        blockchain,
        project,
        version,
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
    FROM {{ dex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
