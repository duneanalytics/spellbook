{{ config(
        alias = 'trades',
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "optimism", "polygon", "bnb", "base", "celo", "avalanche_c"]\',
                                "project",
                                "uniswap",
                                \'["jeff-dude", "mtitus6", "Henrystats", "chrispearcx", "wuligy", "tomfutago", "phu"]\') }}'
        )
}}

{% set uniswap_models = [
ref('uniswap_ethereum_trades')
, ref('uniswap_optimism_trades')
, ref('uniswap_arbitrum_trades')
, ref('uniswap_polygon_trades')
, ref('uniswap_bnb_trades')
, ref('uniswap_base_trades')
, ref('uniswap_celo_trades')
, ref('uniswap_avalanche_c_trades')
] %}


SELECT *
FROM (
    {% for dex_model in uniswap_models %}
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
    FROM {{ dex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)