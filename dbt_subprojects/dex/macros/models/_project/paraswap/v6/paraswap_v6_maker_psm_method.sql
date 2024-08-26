{% macro paraswap_v6_maker_psm_method(table_name ) %}
                SELECT                   
                  call_block_time,
                  call_block_number,
                  call_tx_hash,                  
                  contract_address as project_contract_address,                  
                  call_trace_address,
                  JSON_EXTRACT_SCALAR(makerPSMData, '$.srcToken') as srcToken, 
                  JSON_EXTRACT_SCALAR(makerPSMData, '$.destToken') as destToken, 
                  try_cast(
                    JSON_EXTRACT_SCALAR(makerPSMData, '$.fromAmount') as uint256
                  ) AS fromAmount,
                  try_cast(
                    JSON_EXTRACT_SCALAR(makerPSMData, '$.toAmount') as uint256
                  ) AS toAmount,
                  try_cast(
                    JSON_EXTRACT_SCALAR(makerPSMData, '$.toAmount') as uint256
                  ) AS quotedAmount,
                  output_receivedAmount,
                  JSON_EXTRACT_SCALAR(makerPSMData, '$.metadata') AS metadata,
                  concat(
                    '0x',
                    regexp_replace(
                      try_cast(
                        try_cast(
                          bitwise_and(
                            try_cast(
                              JSON_EXTRACT_SCALAR(makerPSMData, '$.beneficiaryDirectionApproveFlag') AS UINT256
                            ),
                            varbinary_to_uint256 (0xffffffffffffffffffffffffffffffffffffffff)
                          ) as VARBINARY
                        ) as VARCHAR
                      ),
                      '^(0x)?(00){12}' -- shrink hex to get address format (bytes20)
                    ) 
                  ) as beneficiary,
                  0 as partnerAndFee,
                  0 as output_partnerShare,
                  0 as output_paraswapShare,
                  'swapExactAmountInOutOnMakerPSM' as method
                FROM
                     {{ table_name }}                                                        
                WHERE
                  call_success = TRUE
                  {% if is_incremental() %}
                    AND {{ incremental_predicate('call_block_time') }}
                  {% endif %}
{% endmacro %}