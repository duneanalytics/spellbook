{{ config(
	tags=['legacy'],
	
        alias = alias('trades', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum","avalanche_c","optimism","fantom"]\',
                                "project",
                                "curvefi",
                                \'["jeff-dude","yulesa","dsalv","Henrystats","msilb7","ilemi","agaperste"]\') }}'
        )
}}

{% set curvefi_trade_models = [
 ref('curvefi_ethereum_trades_legacy')
,ref('curvefi_optimism_trades_legacy')
,ref('curvefi_avalanche_c_trades_legacy')
,ref('curvefi_fantom_trades_legacy')
] %}


SELECT *
FROM (
    {% for curvefi_model in curvefi_trade_models %}
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
    FROM {{ curvefi_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;
