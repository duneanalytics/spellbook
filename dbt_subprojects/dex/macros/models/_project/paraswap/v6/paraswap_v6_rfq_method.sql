{% macro paraswap_v6_rfq_method(table_name) %}
                SELECT 
                  call_block_time,
                  call_block_number,
                  call_tx_hash,                  
                  contract_address as project_contract_address,
                  call_trace_address,
                  JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(orders[1], '$.order'), '$.makerAsset') as src_token, 
                  JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(orders[1], '$.order'), '$.takerAsset') as dest_token, 
                  try_cast(
                    JSON_EXTRACT_SCALAR(data, '$.from_amount') as uint256
                  ) AS from_amount,
                  try_cast(
                    JSON_EXTRACT_SCALAR(data, '$.to_amount') as uint256
                  ) AS to_amount,
                  try_cast(
                    JSON_EXTRACT_SCALAR(data, '$.to_amount') as uint256
                  ) AS quoted_amount,
                  output_received_amount,
                  JSON_EXTRACT_SCALAR(data, '$.metadata') AS metadata,
                  JSON_EXTRACT_SCALAR(data, '$.beneficiary') AS beneficiary,
                  0 as partnerAndFee,
                  0 as output_partner_share,
                  0 as output_paraswap_share,
                  'swapOnAugustusRFQTryBatchFill' as method
                FROM
                     {{ table_name }}                                                        
                WHERE
                  call_success = TRUE
                  {% if is_incremental() %}
                    AND call_block_time >= date_trunc('day', now() - interval '7' day)
                  {% endif %}
{% endmacro %}