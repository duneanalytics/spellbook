{{ config(
	tags=['legacy'],
	
        alias = alias('offers', legacy_model=True),
        post_hook='{{ expose_spells(\'["optimism", "base"]\',
                                "project",
                                "rubicon",
                                \'["denver"]\') }}'
        )
}}

{% set rubi_models = [
ref('rubicon_optimism_offers_legacy'),
ref('rubicon_arbitrum_offers_legacy'),
ref('rubicon_base_offers_legacy')
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
        block_number, 
        tx_index,
        evt_index, 
        maker, 
        sell_token_symbol,
        buy_token_symbol,
        sell_token_address,
        buy_token_address,
        token_pair,
        sell_amount,
        buy_amount,
        sell_amount_raw,
        buy_amount_raw,
        sold_amount,
        bought_amount,
        sold_amount_raw,
        bought_amount_raw,
        sell_amount_usd,
        buy_amount_usd,
        sold_amount_usd,
        bought_amount_usd,
        txn_cost_usd,
        project_contract_address,
        tx_hash,
        tx_from,
        tx_to
    FROM {{ r_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)