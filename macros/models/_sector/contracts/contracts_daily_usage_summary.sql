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

, trace_txs AS (

  SELECT '{{chain}}' as blockchain, 
  block_date,
  r.to AS contract_address

  FROM {{ source(chain,'traces') }} r
  INNER JOIN  contract_list cl
    ON r.to = cl.contract_address
  
  WHERE 1=1
    AND r.type = 'call'
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_date') }}
    {% endif %}


)

, log_txs AS (

  SELECT '{{chain}}' as blockchain, 
  block_date,
  l.contract_address AS contract_address

  FROM {{ source(chain,'logs') }} l
  INNER JOIN  contract_list cl
    ON l.contract_address = cl.contract_address
  
  WHERE 1=1
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_date') }}
    {% endif %}

)

{% endmacro %}