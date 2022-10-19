{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "dex",
                                \'["jeff-dude", "hosuke", "0xRob"]\') }}'
        )
}}

{% set dex_trade_models = [
'uniswap_trades',
'sushiswap_trades',
'curvefi_ethereum_trades',
'airswap_ethereum_trades',
'clipper_ethereum_trades',
'shibaswap_ethereum_trades'
] %}


SELECT *
FROM (
    {% for dex_model in dex_trade_models %}
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
    FROM {{ ref(dex_model) }}
    {% if not loop.last %}
    UNION
    {% endif %}
    {% endfor %}
)
