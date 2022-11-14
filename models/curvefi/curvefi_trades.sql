{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum","avalanche_c"]\',
                                "project",
                                "curvefi",
                                \'["jeff-dude","yulesa","dsalv","henrystats"]\') }}'
        )
}}

{% set curvefi_trade_models = [
'curvefi_ethereum_trades'
,'curvefi_avalanche_c_trades'
] %}


SELECT *
FROM (
    {% for dex_model in curvefi_trade_models %}
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