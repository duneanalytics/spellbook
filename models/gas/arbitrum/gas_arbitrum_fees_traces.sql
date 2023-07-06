{{ config(
    schema = 'gas_arbitrum',
    alias = alias('fees_traces'),
    tags=['dunesql'],
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'trace'],
    )
}}

WITH traces AS (
     SELECT traces.block_time
     , traces.block_number
     , traces.tx_hash
     , MAX(traces."from") AS trace_from
     , MAX(traces.to) AS trace_to
     , traces.trace
     , MAX(traces.input) AS trace_input
     , bytearray_substring(MAX(traces.input),1,4) AS trace_method
     , SUM(traces.gas_used_original) AS gas_used_original
     , SUM(traces.gas_used_trace) AS gas_used_trace
     , MAX(traces.trace_type) AS trace_type
     , MAX(traces.trace_value) AS trace_value
     , MAX(traces.trace_success) AS trace_success
     , MAX(traces.tx_success) AS tx_success
     FROM (
          SELECT "from"
          , to
          , tx_hash
          , trace_address AS trace
          , cast(gas_used as double) AS gas_used_original
          , cast(gas_used as double) AS gas_used_trace
          , block_time
          , block_number
          , input
          , type AS trace_type
          , call_type
          , value AS trace_value
          , success AS trace_success
          , tx_success
          FROM {{ source('arbitrum','traces') }}
          {% if is_incremental() %}
          WHERE block_time >= date_trunc('day', NOW() - interval '1' day)
          {% endif %}
          
          UNION ALL
          
          SELECT CAST(NULL AS varbinary) AS "from" 
          , CAST(NULL AS varbinary) AS to 
          , tx_hash
          , slice(trace_address, 1, cardinality(trace_address) - 1) AS trace
          , CAST(NULL AS double) AS gas_used_original
          , -cast(gas_used as double) AS gas_used_trace
          , block_time
          , block_number
          , CAST(NULL AS varbinary) AS input
          , CAST(NULL AS varchar) AS trace_type
          , CAST(NULL AS varchar) AS call_type
          , CAST(NULL AS bigint) AS trace_value
          , CAST(NULL AS boolean) AS trace_success
          , CAST(NULL AS boolean) AS tx_success
          FROM {{ source('arbitrum','traces') }}
          WHERE cardinality(trace_address) > 0
          {% if is_incremental() %}
          AND block_time >= date_trunc('day', NOW() - interval '1' day)
          {% endif %}
          ) traces
     GROUP BY traces.tx_hash, traces.trace, traces.block_time, traces.block_number
     )

SELECT 'arbitrum' AS blockchain
, traces.block_time
, date_trunc('day', traces.block_time) AS block_date
, traces.block_number
, traces.tx_hash
, traces.trace_from
, traces.trace_to
, txs."from" AS tx_from
, txs.to AS tx_to
, traces.trace
, traces.trace_method
, bytearray_substring(txs.data,1,4) AS tx_method
, traces.trace_input
, traces.gas_used_original
, traces.gas_used_trace
, txs.gas_used AS tx_gas_used
, cast(traces.gas_used_original as double)/cast(txs.gas_used as double) AS gas_used_original_percentage
, cast(traces.gas_used_trace as double)/cast(txs.gas_used as double) AS gas_used_trace_percentage
, txs.gas_price AS tx_gas_price
, traces.trace_type
, traces.trace_value
, traces.trace_success
, traces.tx_success
, cast(traces.gas_used_original*txs.gas_price as double)/1e18 AS gas_fee_spent_original
, cast(pu.price*traces.gas_used_original*txs.gas_price as double)/1e18 AS gas_fee_spent_original_usd
, cast(traces.gas_used_trace*txs.gas_price as double)/1e18 AS gas_fee_spent_trace
, cast(pu.price*traces.gas_used_trace*txs.gas_price as double)/1e18 AS gas_fee_spent_trace_usd
FROM traces
INNER JOIN {{ source('arbitrum','transactions') }} txs ON txs.block_time=traces.block_time
     AND txs.hash=traces.tx_hash
     {% if is_incremental() %}
     AND txs.block_time >= date_trunc('day', NOW() - interval '1' day)
     {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} pu ON pu.minute=date_trunc('minute', traces.block_time)
     AND pu.blockchain='arbitrum'
     AND pu.contract_address=0x82af49447d8a07e3bd95bd0d56f35241523fbab1
     {% if is_incremental() %}
     AND pu.minute >= date_trunc('day', NOW() - interval '1' day)
     {% endif %}