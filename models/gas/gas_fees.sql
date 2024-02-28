{{ config(
        
        alias = 'fees',
        post_hook='{{ expose_spells(\'["ethereum","bnb","avalanche_c","optimism","arbitrum","zksync","zora","base","scroll"]\',
                                "sector",
                                "gas",
                                \'["soispoke"]\') }}'
        )
}}

{% set gas_fees_models = [
'gas_ethereum_fees',
'gas_bnb_fees',
'gas_avalanche_c_fees',
'gas_optimism_fees',
'gas_arbitrum_fees',
'gas_zksync_fees',
'gas_zora_fees',
'gas_base_fees',
'gas_scroll_fees'
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
