{{ config(
	schema = 'yield_yak',
        alias = 'trades'
        )
}}

{% set yield_yak_models = [
ref('yield_yak_avalanche_c_trades')
,ref('yield_yak_arbitrum_trades')
] %}


SELECT *
FROM (
    {% for dex_model in yield_yak_models %}
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
        trace_address,
        evt_index
    FROM {{ dex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
