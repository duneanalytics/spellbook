{{ config(
        alias = alias('trades'),
        post_hook='{{ expose_spells(\'["ethereum", "gnosis", "avalanche_c", "arbitrum", "fantom", "optimism"]\',
                        "project",
                        "sushiswap",
                        \'["augustog", "hosuke", "Henrystats", "msilb7", "chrispearcx", "codingsh"]\') }}'
        )
}}

{% set sushi_models = [
ref('sushiswap_ethereum_trades')
, ref('sushiswap_avalanche_c_trades')
, ref('sushiswap_gnosis_trades')
, ref('sushiswap_arbitrum_trades')
, ref('sushiswap_fantom_trades')
, ref('sushiswap_optimism_trades')
, ref('sushiswap_polygon_trades')
, ref('sushiswap_bnb_trades')
] %}


SELECT *
FROM (
    {% for dex_model in sushi_models %}
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
; 
