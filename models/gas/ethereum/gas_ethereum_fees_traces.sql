{{ config(
    alias = 'fees_traces',
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
     , MAX(traces.from) AS trace_from
     , MAX(traces.to) AS trace_to
     , traces.trace
     , substring(MAX(traces.input),1,10) AS trace_method
     , SUM(traces.gas_used) AS gas_used
     FROM (
          SELECT from
          , to 
          , tx_hash
          , trace_address AS trace
          , gas_used
          , block_time
          , block_number
          , input
          FROM {{ source('ethereum','traces') }}
          {% if is_incremental() %}
          WHERE block_time >= date_trunc("day", NOW() - interval '1' week)
          {% endif %}
          
          UNION ALL
          
          SELECT CAST(NULL AS varchar(1)) AS from 
          , CAST(NULL AS varchar(1)) AS to 
          , tx_hash
          , slice(trace_address, 1, cardinality(trace_address) - 1) AS trace
          , -gas_used AS gas_used
          , block_time
          , block_number
          , CAST(NULL AS varchar(1)) AS input
          FROM {{ source('ethereum','traces') }}
          WHERE cardinality(trace_address) > 0
          {% if is_incremental() %}
          AND block_time >= date_trunc("day", NOW() - interval '1' week)
          {% endif %}
          ) traces
     GROUP BY traces.tx_hash, traces.trace, traces.block_time, traces.block_number
     )

SELECT 'ethereum' AS blockchain
, traces.block_time
, date_trunc('day', traces.block_time) AS block_date
, traces.block_number
, traces.tx_hash
, traces.trace_from
, traces.trace_to
, txs.from AS tx_from
, txs.to AS tx_to
, traces.trace
, traces.trace_method
, substring(txs.data,1,10) AS tx_method
, traces.gas_used
, txs.gas_used AS tx_gas_used
, traces.gas_used/txs.gas_used AS gas_used_tx_percentage
, txs.gas_price AS tx_gas_price
, (traces.gas_used*txs.gas_price)/POWER(10, 18) AS spent_gas_fee
, (pu.price*traces.gas_used*txs.gas_price)/POWER(10, 18) AS spent_gas_fee_usd
FROM traces
LEFT JOIN {{ source('ethereum','transactions') }} txs ON txs.block_time=traces.block_time
     AND txs.hash=traces.tx_hash
LEFT JOIN {{ source('prices', 'usd') }} pu ON pu.minute=date_trunc('minute', traces.block_time)
     AND pu.blockchain='ethereum'
     AND pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
     {% if is_incremental() %}
     AND pu.minute >= date_trunc("day", NOW() - interval '1' week)
     {% endif %}