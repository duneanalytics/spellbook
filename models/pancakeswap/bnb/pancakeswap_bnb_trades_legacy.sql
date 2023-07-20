{{ config(
	    tags=['legacy'],
        alias = alias('trades', legacy_model=True)
        )
}}


{% set pancake_models = [
    ref('pancakeswap_v2_bnb_amm_trades_legacy')
,   ref('pancakeswap_v2_bnb_mmpool_trades_legacy')
,   ref('pancakeswap_v2_bnb_stableswap_trades_legacy')
,   ref('pancakeswap_v3_bnb_amm_trades_legacy')
] %}


SELECT *
FROM (
    {% for dex_model in pancake_models %}
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
    FROM {{ dex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;