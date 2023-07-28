{{ config(tags=['chainlink', 'CCIP'],
        alias = alias('CCIP_events'),
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

{% set chainlink_models_off = [
ref('chainlink_ethereum_ccip_offramps')
, ref('chainlink_optimism_ccip_offramps')
, ref('chainlink_avalanche_c_ccip_offramps')
, ref('chainlink_polygon_ccip_offramps')

] %}


WITH 
onRamp_base as (

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
        , contract_address as origin_contract_address
        , evt_tx_hash as origin_evt_tx_hash
        , evt_block_time as origin_evt_block_time
        , evt_index as origin_evt_index
        , evt_block_number as origin_evt_block_number


    
    FROM {{ onramps }}
)
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}

)

, offRamp_base as (

SELECT *
FROM (
    {% for offramps in chainlink_models_off %}
    SELECT
        origin
        , destination
        , contract_address as destination_contract_address
        , evt_tx_hash as destination_evt_tx_hash
        , evt_index as destination_evt_index
        , evt_block_time as destination_evt_block_time
        , evt_block_number as destination_evt_block_number
        , messageId
        , returnData as destination_returnData
        , sequenceNumber as destination_sequenceNumber
        , state as destination_state

    
    FROM {{ offramps }}
)
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    
)

SELECT 
    onramp.origin
    , onramp.destination
    , onramp.feeTokenAmount
    , onramp.sender
    , onramp.receiver
    , onramp.nonce
    , onramp.gasLimit
    , onramp.strict
    , onramp.data
    , offramp.destination_returnData
    , onramp.tokenAmounts
    , onramp.feeToken
    , offramp.destination_state
    , onramp.messageId
    , onramp.router
    , onramp.origin_contract_address
    , offramp.destination_contract_address
    , onramp.origin_evt_tx_hash
    , offramp.destination_evt_tx_hash
    , onramp.origin_evt_block_time
    , offramp.destination_evt_block_time
    , onramp.origin_evt_index
    , offramp.destination_evt_index
    , onramp.origin_evt_block_number
    , offramp.destination_evt_block_number
    , offramp.destination_sequenceNumber
FROM onRamp_base as onramp
LEFT JOIN  offRamp_base as offramp on cast(onramp.messageId as VARCHAR) = cast(offramp.messageId as VARCHAR)