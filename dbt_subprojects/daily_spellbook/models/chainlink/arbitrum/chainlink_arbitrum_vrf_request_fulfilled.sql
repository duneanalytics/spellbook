{{
  config(
    
    alias='vrf_request_fulfilled',
    materialized='view'
  )
}}

SELECT
    'arbitrum' as blockchain,
    MAX(bytearray_to_uint256(bytearray_substring(v1_request.data, 110, 19)) / 1e18) AS token_value,
    MAX(v1_fulfilled.tx_from) as operator_address,
    MAX(v1_fulfilled.block_time) as evt_block_time
FROM
    {{ ref('chainlink_arbitrum_vrf_v1_random_request_logs') }} v1_request
    INNER JOIN {{ ref('chainlink_arbitrum_vrf_v1_random_fulfilled_logs') }} v1_fulfilled ON bytearray_substring(v1_fulfilled.data, 1, 32) = bytearray_substring(v1_request.data, 129, 32)
    GROUP BY
        v1_request.tx_hash,
        v1_fulfilled.tx_from
UNION

SELECT
    'arbitrum' as blockchain,
    MAX(bytearray_to_uint256(bytearray_substring(v2_fulfilled.data, 33, 32)) / 1e18) AS token_value,
    MAX(v2_fulfilled.tx_from) as operator_address,
    MAX(v2_fulfilled.block_time) as evt_block_time
FROM
    {{ ref('chainlink_arbitrum_vrf_v2_random_fulfilled_logs') }} v2_fulfilled
    GROUP BY
        v2_fulfilled.tx_hash,
        v2_fulfilled.tx_from,
        v2_fulfilled.index
