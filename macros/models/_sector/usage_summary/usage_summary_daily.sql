{% macro usage_summary_daily( chain, days_forward=365 ) %}


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

WITH check_date AS (
  SELECT
  {% if is_incremental() %}
    MAX(block_date) AS base_time FROM {{this}}
  {% else %}
    MIN(block_time) AS base_time FROM {{ source( chain , 'transactions') }}
  {% endif %}
)

  SELECT
    COALESCE(tr.blockchain, lo.blockchain) AS blockchain
  , COALESCE(tr.block_date, lo.block_date) AS block_date
  , COALESCE(tr.block_month, lo.block_month) AS block_month
  , COALESCE(tr.address, lo.address) AS address
  ---
  , COALESCE(num_tx_to_blocks, 0) AS num_to_blocks
  , COALESCE(num_tx_tos, 0) AS num_tx_tos
  , COALESCE(num_tx_to_senders, 0) AS num_tx_to_senders
  , COALESCE(sum_tx_to_gas_used, 0) AS sum_tx_to_gas_used

  , COALESCE(sum_tx_to_calldata_bytes, 0) AS sum_tx_to_calldata_bytes
  , COALESCE(sum_tx_to_calldata_gas, 0) AS sum_tx_to_calldata_gas

  , COALESCE(sum_tx_to_gas_fee, 0) AS sum_tx_to_gas_fee
  ---
  , COALESCE(num_trace_to_blocks, 0) AS num_trace_to_blocks
  , COALESCE(num_trace_to_txs, 0) AS num_trace_to_txs
  , COALESCE(num_trace_to_calls, 0) AS num_trace_to_calls

  , COALESCE(num_trace_to_tx_senders, 0) AS num_trace_to_tx_senders
  , COALESCE(num_trace_to_call_senders, 0) AS num_trace_to_call_senders

  , COALESCE(sum_trace_to_gas_used, 0) AS sum_trace_to_gas_used
  , COALESCE(sum_trace_to_tx_gas_used, 0) AS sum_trace_to_tx_gas_used

  , COALESCE(sum_trace_to_tx_gas_fee, 0) AS sum_trace_to_tx_gas_fee
  --
  , COALESCE(num_logs_emitted_blocks, 0) AS num_logs_emitted_blocks
  , COALESCE(num_logs_emitted_txs, 0) AS num_logs_emitted_txs
  , COALESCE(num_logs_emitted_events, 0) AS num_logs_emitted_events

  , COALESCE(num_logs_emitted_tx_senders, 0) AS num_logs_emitted_tx_senders
  , COALESCE(sum_logs_emitted_tx_gas_used, 0) AS sum_logs_emitted_tx_gas_used

  FROM {{ ref('usage_summary_' + chain + '_daily_traces') }} tr
  cross join check_date cd
  
  LEFT JOIN {{ ref('usage_summary_' + chain + '_daily_transactions') }} tt -- all txs to have an associated trace
    ON tr.blockchain = tt.blockchain
    AND tr.block_date = tt.block_date
    AND tr.block_month = tt.block_month
    AND tr.address = tt.address
    AND {{ incremental_days_forward_predicate('tt.block_date', 'cd.base_time', days_forward, 'day') }}

  FULL OUTER JOIN {{ ref('usage_summary_' + chain + '_daily_logs') }} lo
    ON tr.blockchain = lo.blockchain
    AND tr.block_date = lo.block_date
    AND tr.block_month = lo.block_month
    AND tr.address = lo.address
    AND {{ incremental_days_forward_predicate('lo.block_date', 'cd.base_time', days_forward, 'day') }}
  
  WHERE 1=1
    AND {{ incremental_days_forward_predicate('tr.block_date', 'cd.base_time', days_forward, 'day') }}

{% endmacro %}