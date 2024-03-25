{{
  config(
    
    alias='ccip_send_requested_logs_v1_2',
    materialized='view'
  )
}}

SELECT
  'avalanche_c' as blockchain,
  block_hash,
  contract_address,
  data,
  topic0,
  topic1,
  topic2,
  topic3,
  tx_hash,
  block_number,
  block_time,
  index,
  tx_index,
  tx_from,
  bytearray_to_uint256(bytearray_substring(data, 289, 32)) as fee_token_amount,
  bytearray_to_uint256(bytearray_substring(data, 33, 32)) as origin_selector,
  varbinary_ltrim(bytearray_substring(data, 257, 32)) as fee_token,
  onramp_meta.chain_selector as destination_selector,
  onramp_meta.blockchain as destination_blockchain
FROM
  {{ source('avalanche_c', 'logs') }} logs
left join {{ref('chainlink_avalanche_c_ccip_onramp_meta')}} onramp_meta on onramp_meta.onramp = contract_address
WHERE
  topic0 = 0xd0c3c799bf9e2639de44391e7f524d229b2b55f5b1ea94b2bf7da42f7243dddd -- CCIPSendRequested v1.2.0