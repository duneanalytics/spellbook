{% macro paraswap_v6_uniswaplike_method(table_name, method_name, data_field, extra_field=None) %}
                SELECT
                  call_block_time,
                  call_block_number,
                  call_tx_hash,
                  contract_address as project_contract_address,
                  call_trace_address,
                  JSON_EXTRACT_SCALAR({{ data_field }}, '$.src_token') AS src_token,
                  JSON_EXTRACT_SCALAR({{ data_field }}, '$.dest_token') AS dest_token,
                  try_cast(
                    JSON_EXTRACT_SCALAR({{ data_field }}, '$.from_amount') as uint256
                  ) AS from_amount,
                  try_cast(
                    JSON_EXTRACT_SCALAR({{ data_field }}, '$.to_amount') as uint256
                  ) AS to_amount,
                  try_cast(
                    JSON_EXTRACT_SCALAR({{ data_field }}, '$.quoted_amount') as uint256
                  ) AS quoted_amount,
                  output_received_amount,
                  JSON_EXTRACT_SCALAR({{ data_field }}, '$.metadata') AS metadata,
                  JSON_EXTRACT_SCALAR({{ data_field }}, '$.beneficiary') AS beneficiary,
                  partnerAndFee,
                  output_partner_share,
                  output_paraswap_share,
                  '{{ method_name }}' as method{% if extra_field %},
                  {{ extra_field }}{% endif %}
                FROM
                  {{ table_name }}
                WHERE
                  call_success = TRUE
                  {% if is_incremental() %}
                  AND {{ incremental_predicate('call_block_time') }}
                  {% endif %}
{% endmacro %}
