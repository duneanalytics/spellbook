{% macro paraswap_v6_uniswaplike_method(table_name, method_name, data_field, extra_field=None) %}
                SELECT                
                  call_block_time,
                  call_block_number,
                  call_tx_hash,                  
                  contract_address as project_contract_address,
                  call_trace_address,
                  JSON_EXTRACT_SCALAR({{ data_field }}, '$.srcToken') AS srcToken,
                  JSON_EXTRACT_SCALAR({{ data_field }}, '$.destToken') AS destToken,
                  try_cast(
                    JSON_EXTRACT_SCALAR({{ data_field }}, '$.fromAmount') as uint256
                  ) AS fromAmount,
                  try_cast(
                    JSON_EXTRACT_SCALAR({{ data_field }}, '$.toAmount') as uint256
                  ) AS toAmount,
                  try_cast(
                    JSON_EXTRACT_SCALAR({{ data_field }}, '$.quotedAmount') as uint256
                  ) AS quotedAmount,
                  output_receivedAmount,
                  JSON_EXTRACT_SCALAR({{ data_field }}, '$.metadata') AS metadata,
                  JSON_EXTRACT_SCALAR({{ data_field }}, '$.beneficiary') AS beneficiary,
                  partnerAndFee,
                  output_partnerShare,
                  output_paraswapShare,
                  '{{ method_name }}' as method{% if extra_field %},
                  {{ extra_field }}{% endif %}                  
                FROM
                  {{ table_name }}                  
                WHERE
                  call_success = TRUE
                  {% if is_incremental() %}
                    AND call_block_time >= date_trunc('day', now() - interval '7' day)
                  {% endif %}
{% endmacro %}
