{{ config(tags=['chainlink', 'CCIP'],
        alias = alias('CCIP_onRamps'),
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
        , sourceChainSelector
        , sequenceNumber
        , feeTokenAmount
        , sender
        , receiver
        , nonce
        , gasLimit
        , strict
        , data
        , tokenAmounts
        , feeToken
        , messageId
        , router
        , contract_address
        , evt_tx_hash 
        , evt_block_time 
        , evt_index 
        , evt_block_number 


    
    FROM {{ onramps }}
)
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}


