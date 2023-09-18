{{ config(
        tags=['dunesql'],
        alias = alias('ccip_onramps'),
        post_hook='{{ expose_spells(\'["ethereum", "avalanche_c", "optimism", "polygon"]\',
                                "project",
                                "chainlink",
                                \'["synthquest"]\') }}'
        )
}}

{% set chainlink_models_on = [
ref('chainlink_ethereum_ccip_onramps')
, ref('chainlink_optimism_ccip_onramps')
, ref('chainlink_avalanche_c_ccip_onramps')
, ref('chainlink_polygon_ccip_onramps')

] %}


SELECT *
FROM (
    {% for onramps in chainlink_models_on %}
    SELECT
        origin
        , destination
        , source_chain_selector
        , sequence_number
        , fee_token_amount
        , sender
        , receiver
        , nonce
        , gas_limit
        , strict
        , data
        , token_amounts
        , fee_token
        , message_id
        , router
        , contract_address
        , evt_tx_hash 
        , evt_block_time 
        , evt_index 
        , evt_block_number
    FROM {{ onramps }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)


