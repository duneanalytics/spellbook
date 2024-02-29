{{ config(
	    
        alias = 'trades',
        post_hook='{{ expose_spells(\'["avalanche_c","fantom","arbitrum","bnb","ethereum","optimism","polygon","base"]\',
                                "project",
                                "paraswap",
                                \'["Henrystats","springzh"]\') }}'
        )
}}

{% set paraswap_models = [
ref('paraswap_avalanche_c_trades')
,ref('paraswap_fantom_trades')
,ref('paraswap_arbitrum_trades')
,ref('paraswap_bnb_trades')
,ref('paraswap_ethereum_trades')
,ref('paraswap_optimism_trades')
,ref('paraswap_polygon_trades')
,ref('paraswap_base_trades')
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
        trace_address,
        evt_index
    FROM {{ dex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

