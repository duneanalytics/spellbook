{{ config(
	tags=['legacy'],
	
        alias = alias('fees', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum","bnb","avalanche_c","optimism","arbitrum"]\',
                                "sector",
                                "gas",
                                \'["soispoke"]\') }}'
        )
}}

{% set gas_fees_models = [
'gas_ethereum_fees_legacy',
'gas_bnb_fees_legacy',
'gas_avalanche_c_fees_legacy',
'gas_optimism_fees_legacy',
'gas_arbitrum_fees_legacy'
] %}

SELECT *
FROM (
    {% for gas_model in gas_fees_models %}
    SELECT
        blockchain,
        block_number,
        block_time,
        block_date,
        tx_hash,
        tx_sender, 
        tx_receiver,
        native_token_symbol,
        tx_amount_native,
        tx_amount_usd,
        tx_fee_native,
        tx_fee_usd,
        burned_native,
        burned_usd,
        validator,
        gas_price_gwei,
        gas_price_usd,
        gas_used,
        gas_limit,
        gas_usage_percent,
        transaction_type
    FROM {{ ref(gas_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
