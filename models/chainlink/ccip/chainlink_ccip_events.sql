{{ config(
        tags=['dunesql'],
        alias = alias('ccip_events'),
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
onramp_base as (

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
        , contract_address as origin_contract_address
        , evt_tx_hash as origin_evt_tx_hash
        , evt_block_time as origin_evt_block_time
        , evt_index as origin_evt_index
        , evt_block_number as origin_evt_block_number 
    FROM {{ onramps }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
)

, offramp_base as (

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
        , message_id
        , return_data as destination_return_data
        , sequence_number as destination_sequence_number
        , state as destination_state
    FROM {{ offramps }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
)

SELECT 
    onramp.origin
    , onramp.destination
    , onramp.fee_token_amount
    , onramp.sender
    , onramp.receiver
    , onramp.nonce
    , onramp.gas_limit
    , onramp.strict
    , onramp.data
    , offramp.destination_return_data
    , onramp.token_amounts
    , onramp.fee_token
    , offramp.destination_state
    , onramp.message_id
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
    , offramp.destination_sequence_number
FROM onramp_base as onramp
LEFT JOIN  offramp_base as offramp on cast(onramp.message_id as VARCHAR) = cast(offramp.message_id as VARCHAR)