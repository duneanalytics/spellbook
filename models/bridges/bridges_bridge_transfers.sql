{{ config(
        alias ='bridge_transfers',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "bridges",
                                \'["msilb7"]\') }}'
        )
}}

{% set bridge_transfer_models = [
'hop_protocol_bridge_transfers'
] %}


SELECT *
FROM (
    {% for bridge_model in bridge_transfer_models %}
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
    UNION ALL
    {% endif %}
    {% endfor %}
)
