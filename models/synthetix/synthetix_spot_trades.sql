{{ config(
        tags=['dunesql'],
        alias = alias('spot_trades'),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "synthetix",
                                \'["msilb7"]\') }}'
        )
}}

{% set snx_models = [
ref('synthetix_optimism_spot_trades')
] %}


SELECT *
FROM (
    {% for snx_model in snx_models %}
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
    FROM {{ snx_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;