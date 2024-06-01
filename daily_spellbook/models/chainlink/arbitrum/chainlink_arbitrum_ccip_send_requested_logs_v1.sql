{{
  config(
    
    alias='ccip_send_requested_logs_v1',
    materialized='view'
  )
}}

SELECT
  'arbitrum' as blockchain,
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
  bytearray_to_uint256(bytearray_substring(data, 97, 32)) as fee_token_amount,
  bytearray_to_uint256(bytearray_substring(data, 33, 32)) as origin_selector,
  varbinary_ltrim(bytearray_substring(data, 353, 32)) as fee_token,
  onramp_meta.chain_selector as destination_selector,
  onramp_meta.blockchain as destination_blockchain
FROM
  {{ source('arbitrum', 'logs') }} logs
left join {{ref('chainlink_arbitrum_ccip_onramp_meta')}} onramp_meta on onramp_meta.onramp = contract_address
WHERE
  topic0 = 0xaffc45517195d6499808c643bd4a7b0ffeedf95bea5852840d7bfcf63f59e821 -- CCIPSendRequested v1.0.0