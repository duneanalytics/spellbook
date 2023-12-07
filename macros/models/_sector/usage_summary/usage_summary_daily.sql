{% macro usage_summary_daily( chain ) %}


/*
Goal: Provide a table at daily granularity that abstracts away the need to go
through transactions, traces, logs tables in order to generate aggregate stats

Stats:
- # of Transactions Called (Trace - Distinct tx hash)
- # of Event Emitting Transactions (Logs - Distinct tx hash)
- Total Gas Used at Trace-Level (Trace Gas Used) - TBD if we can also integrate the gas spell
- Total Gas Used at Transaction Level (Transaction Gas Used)
- # of Contracts Calling (Trace From)
- # of Tx Senders Calling (Transaction From - When Trace)
- # of Tx Senders Emitting (Transaction From - When Log)


This should only be joined to other contracts tables at the query stage; however, we may read from
the `base` spell unified with `predeploys`, as our identifier for the total set of contracts to evaluate
, or keep this spell agnostic to contracts vs EOAs, and determine this at the query level.
*/

  SELECT
    COALESCE(tr.blockchain, lo.blockchain) AS blockchain
  , COALESCE(tr.block_date, lo.block_date) AS block_date
  , COALESCE(tr.block_month, lo.block_month) AS block_month
  , COALESCE(tr.address, lo.address) AS address
  ---
  , COALESCE(num_to_blocks, 0) AS num_to_blocks
  , COALESCE(num_to_txs, 0) AS num_to_txs
  , COALESCE(num_to_tx_senders, 0) AS num_to_tx_senders
  , COALESCE(sum_to_tx_gas_used, 0) AS sum_to_tx_gas_used
  , COALESCE(sum_to_tx_calldata_bytes, 0) AS sum_to_tx_calldata_bytes
  , COALESCE(sum_to_tx_calldata_gas, 0) AS sum_to_tx_calldata_gas
  ---
  , COALESCE(num_trace_blocks, 0) AS num_trace_blocks
  , COALESCE(num_trace_txs, 0) AS num_trace_txs
  , COALESCE(num_trace_calls, 0) AS num_trace_calls

  , COALESCE(num_trace_tx_senders, 0) AS num_trace_tx_senders
  , COALESCE(num_trace_call_senders, 0) AS num_trace_call_senders

  , COALESCE(sum_trace_gas_used, 0) AS sum_trace_gas_used
  , COALESCE(sum_trace_tx_gas_used, 0) AS sum_trace_tx_gas_used
  --
  , COALESCE(num_log_blocks, 0) AS num_log_blocks
  , COALESCE(num_log_txs, 0) AS num_log_txs
  , COALESCE(num_log_events, 0) AS num_log_events

  , COALESCE(num_log_tx_senders, 0) AS num_log_tx_senders
  , COALESCE(sum_log_tx_gas_used, 0) AS sum_log_tx_gas_used

  FROM {{ ref('usage_summary_' + chain + '_daily_traces') }} tr
  LEFT JOIN {{ ref('usage_summary_' + chain + '_daily_transactions') }} tt -- all txs to have an associated trace
    ON tr.blockchain = tt.blockchain
    AND tr.block_date = tt.block_date
    AND tr.block_month = tt.block_month
    AND tr.address = tt.address
    {% if is_incremental() %}
    AND tt.block_date >= DATE_TRUNC('day', NOW() - interval '1' day) --ensure we capture whole days, with 1 day buffer depending on spell runtime
    {% endif %}
  FULL OUTER JOIN {{ ref('usage_summary_' + chain + '_daily_logs') }} lo
    ON tr.blockchain = lo.blockchain
    AND tr.block_date = lo.block_date
    AND tr.block_month = lo.block_month
    AND tr.address = lo.address
    {% if is_incremental() %}
    AND lo.block_date >= DATE_TRUNC('day', NOW() - interval '1' day) --ensure we capture whole days, with 1 day buffer depending on spell runtime
    {% endif %}
  
  WHERE 1=1
  {% if is_incremental() %}
  AND tr.block_date >= DATE_TRUNC('day', NOW() - interval '1' day) --ensure we capture whole days, with 1 day buffer depending on spell runtime
  {% endif %}

{% endmacro %}