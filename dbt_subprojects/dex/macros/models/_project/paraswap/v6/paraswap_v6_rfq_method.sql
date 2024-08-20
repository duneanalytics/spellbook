{% macro paraswap_v6_rfq_method(table_name) %}
                SELECT 
                  call_block_time,
                  call_block_number,
                  call_tx_hash,                  
                  contract_address as project_contract_address,                  
                  call_trace_address,
                  JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(orders[1], '$.order'), '$.takerAsset') as srcToken, 
                  JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(orders[1], '$.order'), '$.makerAsset') as destToken, 
                  try_cast(
                    JSON_EXTRACT_SCALAR(data, '$.fromAmount') as uint256
                  ) AS fromAmount,
                  try_cast(
                    JSON_EXTRACT_SCALAR(data, '$.toAmount') as uint256
                  ) AS toAmount,
                  try_cast(
                    JSON_EXTRACT_SCALAR(data, '$.toAmount') as uint256
                  ) AS quotedAmount,
                  output_receivedAmount,
                  JSON_EXTRACT_SCALAR(data, '$.metadata') AS metadata,
                  JSON_EXTRACT_SCALAR(data, '$.beneficiary') AS beneficiary,
                  0 as partnerAndFee,
                  0 as output_partnerShare,
                  0 as output_paraswapShare,
                  'swapOnAugustusRFQTryBatchFill' as method
                FROM
                     {{ table_name }}                                                        
                WHERE
                  call_success = TRUE
                  {% if is_incremental() %}
                    AND {{ incremental_predicate('call_block_time') }}
                  {% endif %}
{% endmacro %}