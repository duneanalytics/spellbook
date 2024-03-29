{{ config(
        alias = 'trades',
        
        )
}}

{% set paraswap_models = [
ref('paraswap_v5_avalanche_c_trades')
,ref('paraswap_v6_avalanche_c_trades')
] %}


SELECT *
FROM (
    {% for dex_model in paraswap_models %}
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
        trace_address, --ensure field is explicitly cast as array<bigint> in base models
        evt_index
    FROM {{ dex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;