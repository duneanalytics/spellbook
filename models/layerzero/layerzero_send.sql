{{ config(
    schema='layerzero',
    alias = alias('send'),
    post_hook='{{ expose_spells(\'["ethereum", "gnosis", "avalanche_c", "arbitrum", "fantom", "optimism", "bnb", "polygon"]\',
                              "project",
                              "layerzero",
                              \'["bennyfeng"]\') }}'
    )
}}

{% set layerzero_models = [
ref('layerzero_arbitrum_send')
, ref('layerzero_avalanche_c_send')
, ref('layerzero_bnb_send')
, ref('layerzero_ethereum_send')
, ref('layerzero_fantom_send')
, ref('layerzero_gnosis_send')
, ref('layerzero_optimism_send')
, ref('layerzero_polygon_send')
] %}

SELECT *
FROM (
    {% for model in layerzero_models %}
    SELECT
        blockchain,
        source_chain_id,
        source_chain_name,
        destination_chain_id,
        destination_chain_name,
        tx_hash,
        block_number,
        endpoint_contract,
        block_time,
        trace_address,
        adapter_params,
        refund_address,
        zro_payment_address,
        user_address,
        transaction_contract,
        source_bridge_contract,
        destination_bridge_contract,
        transfer_type,
        currency_symbol,
        currency_contract,
        amount_usd,
        amount_original,
        amount_raw
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
