{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "odos",
                                \'["Henrystats"]\') }}'
        )
}}

{% set odos_models = [
ref('odos_avalanche_c_trades')
] %}


SELECT *
FROM (
    {% for aggregator_model in odos_models %}
    SELECT
        blockchain,
        project,
        version,
        block_date,
        block_time,
        amount_usd,
        tokens_bought,
        tokens_sold,
        taker,
        maker,
        project_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        trace_address, --ensure field is explicitly cast as array<bigint> in base models
        evt_index
    FROM {{ aggregator_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;
