{% macro usage_summary_daily_transactions( chain, start_date = '2015-01-01', end_date = '9999-12-31' ) %}

SELECT
    '{{chain}}' as blockchain
  , block_date
  , DATE_TRUNC('month', block_date ) AS block_month
  , COALESCE(t.to, 0x) AS address
  , COUNT(DISTINCT block_number) AS num_tx_to_blocks
  , COUNT(*) AS num_tx_tos
  , COUNT(DISTINCT "from") AS num_tx_to_senders
  , SUM(gas_used) AS sum_tx_to_gas_used
  , SUM(bytearray_length(t.data)) AS sum_tx_to_calldata_bytes
  , SUM(
    {{ evm_get_calldata_gas_from_data('t.data') }}
  ) AS sum_tx_to_calldata_gas
  FROM {{ source(chain,'transactions') }} t

  WHERE 1=1
    {% if is_incremental() %}
    AND t.block_date >= DATE_TRUNC('day', NOW() - interval '1' day) --ensure we capture whole days, with 1 day buffer depending on spell runtime
    -- AND [[ incremental_predicate('t.block_date') ]]
    {% endif %}
    AND t.block_date BETWEEN cast( '{{start_date}}' as date) AND cast( '{{end_date}}' as date)
    AND t.success
  GROUP BY 1,2,3,4

{% endmacro %}