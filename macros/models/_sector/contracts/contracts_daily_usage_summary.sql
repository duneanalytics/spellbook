{% macro contracts_daily_usage_summary( chain ) %}


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

with contract_list AS (
  SELECT contract_address FROM {{ref('contracts_' + chain + '_base_starting_level') }}
    UNION ALL
  SELECT contract_address FROM {{ ref('contracts_predeploys') }} WHERE blockchain = '{{chain}}'
)

, to_txs AS (
SELECT
    '{{chain}}' as blockchain
  , block_date
  , t.to AS contract_address
  , COUNT(DISTINCT block_number) AS num_to_blocks
  , COUNT(*) AS num_to_txs
  , COUNT(DISTINCT "from") AS num_to_tx_senders
  , SUM(gas_used) AS sum_to_tx_gas_used
  , SUM(bytearray_length(t.data)) AS sum_to_tx_calldata_bytes
  , SUM(
    16 * ( bytearray_length(data) - (length(from_utf8(data)) - length(replace(from_utf8(data), chr(0), ''))) ) --nonzero bytes
    + 4 * ( (length(from_utf8(data)) - length(replace(from_utf8(data), chr(0), ''))) )
  ) AS sum_to_tx_calldata_gas
  FROM {{ source(chain,'transactions') }} t
  INNER JOIN contract_list cl
          ON r.to = cl.contract_address
  WHERE {{ incremental_predicate('t.block_date') }} 
  GROUP BY 1,2,3
)

, trace_txs AS (

  SELECT
    '{{chain}}' as blockchain
  , block_date
  , a.to AS contract_address
  ---
  , COUNT(DISTINCT block_number) AS num_trace_blocks
  , COUNT(DISTINCT tx_hash) AS num_trace_txs
  , COUNT(*) AS num_trace_calls
  ---
  , COUNT(DISTINCT tx_from) AS num_trace_tx_senders
  , COUNT(DISTINCT "from") AS num_trace_call_senders
  ---
  , SUM(a.gas_used) AS sum_trace_gas_used
  /* maybe add the trace gas with subtraces removed here? */
  , SUM(CASE WHEN trace_number = 1 THEN tx_gas_used ELSE 0 END) AS sum_trace_tx_gas_used

  FROM (
      SELECT r.block_date, r.block_number, r.to, r.tx_hash, r.tx_from, r."from"
        , r.gas_used, t.gas_used as tx_gas_used
        , ROW_NUMBER() OVER (PARTITION BY r.tx_hash) AS trace_number --reindex trace to ensure single count

        FROM {{ source(chain,'traces') }} r
        INNER JOIN  {{ source(chain,'transactions') }} t 
          ON t.hash = r.tx_hash
          AND t.block_number = r.tx_block_number
          AND t.block_date = r.block_date
          AND {{ incremental_predicate('t.block_date') }} 
        INNER JOIN contract_list cl
          ON r.to = cl.contract_address
        
        WHERE 1=1
          AND r.type = 'call'
          AND r.success AND r.tx_success
          {% if is_incremental() %}
          AND {{ incremental_predicate('r.block_date') }}
          AND {{ incremental_predicate('t.block_date') }}
          {% endif %}
      GROUP BY 1,2,3,4,5,6,7,8
    ) a
  GROUP BY 1,2,3
)

, log_txs AS (

  SELECT
    '{{chain}}' as blockchain
  , block_date
  , l.contract_address AS contract_address
  ---
  , COUNT(DISTINCT block_number) AS num_log_blocks
  , COUNT(DISTINCT tx_hash) AS num_log_txs
  , COUNT(*) AS num_log_events
  ---
  , COUNT(DISTINCT tx_from) AS num_log_tx_senders
  , SUM(CASE WHEN log_number = 1 THEN tx_gas_used ELSE 0 END) AS sum_log_tx_gas_used
  

  FROM (
      SELECT l.block_date, l.block_number, l.contract_address, l.tx_hash, l.tx_from,
        , t.gas_used as tx_gas_used
        , ROW_NUMBER() OVER (PARTITION BY l.tx_hash) AS log_number --reindex log to ensure single count

        FROM {{ source(chain,'logs') }} l
        INNER JOIN  {{ source(chain,'transactions') }} t 
          ON t.hash = r.tx_hash
          AND t.block_number = l.tx_block_number
          AND t.block_date = l.block_date
          AND {{ incremental_predicate('t.block_date') }} 
        INNER JOIN contract_list cl
          ON l.contract_address = cl.contract_address
        
        WHERE 1=1
          AND r.type = 'call'
          AND r.success AND r.tx_success
          {% if is_incremental() %}
          AND {{ incremental_predicate('l.block_date') }}
          AND {{ incremental_predicate('t.block_date') }}
          {% endif %}
      GROUP BY 1,2,3,4,5,6
    ) a
  GROUP BY 1,2,3

)

SELECT 
  DATE_TRUNC('month', block_date ) AS block_month
  , *
FROM (
  SELECT
    COALESCE(tr.blockchain, lo.blockchain) AS blockchain
  , COALESCE(tr.block_date, lo.block_date) AS block_date
  , COALESCE(tr.contract_address, lo.contract_address) AS contract_address
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

  FROM trace_txs tr
  LEFT JOIN to_txs tt -- all txs to have an associated trace
    ON tr.blockchain = tt.blockchain
    AND tr.block_date = tt.block_date
    AND tr.contract_address = tt.contract_address
  FULL OUTER JOIN log_txs lo
    ON tr.blockchain = lo.blockchain
    AND tr.block_date = lo.block_date
    AND tr.contract_address = lo.contract_address
  
  ) a
{% endmacro %}