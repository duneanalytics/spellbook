{{ config(
        tags=['dunesql'],
        alias = alias('ccip_offramps'),
        post_hook='{{ expose_spells(\'["ethereum", "avalanche_c", "optimism", "polygon"]\',
                                "project",
                                "chainlink",
                                \'["synthquest"]\') }}'
        )
}}

{% set chainlink_models_off = [
ref('chainlink_ethereum_ccip_offramps')
, ref('chainlink_optimism_ccip_offramps')
, ref('chainlink_avalanche_c_ccip_offramps')
, ref('chainlink_polygon_ccip_offramps')

] %}




SELECT *
FROM (
    {% for offramps in chainlink_models_off %}
    SELECT
        origin
        , destination
        , contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , messageId
        , returnData
        , sequenceNumber
        , state

    
    FROM {{ offramps }}
)
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}


