{{ config(
        alias='trades',
        post_hook='{{ expose_spells(\'["fantom", "optimism"]\',
                        "project",
                        "beethoven_x",
                        \'["Henrystats", "msilb7"]\') }}'
        )
}}

{% set beets_models = [
ref('beethoven_x_optimism_trades')
,ref('beethoven_x_fantom_trades')
] %}


SELECT *
FROM (
    {% for beet_model in beets_models %}
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
    FROM {{ beet_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;