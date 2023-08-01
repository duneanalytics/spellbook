{{ config(
	tags=['legacy'],
	
        alias = alias('trades', legacy_model=True),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "rubicon",
                                \'["msilb7"]\') }}'
        )
}}

{% set rubi_models = [
ref('rubicon_optimism_trades_legacy'),
ref('rubicon_arbitrum_trades_legacy')
] %}


SELECT *
FROM (
    {% for r_model in rubi_models %}
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
        CAST(token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw,
        CAST(token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw,
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
    FROM {{ r_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;