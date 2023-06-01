{{ config(
    alias = 'fees_traces',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'trace'],
    )
}}

SELECT 'ethereum' AS blockchain
, etg.block_time
, date_trunc('day', etg.block_time) AS block_date
, etg.block_number
, etg.tx_hash
, MAX(etg.to) AS trace_to
, etg.trace
, SUM(etg.gas_used) AS gas_used
, (SUM(etg.gas_used)*etc.gas_price)/1e18 AS spent_gas_fee
, (AVG(pu.price)*SUM(etg.gas_used)*etc.gas_price)/POWER(10, 18) AS spent_gas_fee_usd
FROM (
     SELECT et.to 
     , et.tx_hash
     , et.trace_address AS trace
     , et.gas_used
     , et.block_time
     , et.block_number
     FROM {{ source('ethereum','traces') }} et
     WHERE et.to IS NOT NULL
     {% if is_incremental() %}
     AND et.block_time >= date_trunc("day", NOW() - interval '1' week)
     {% endif %}
     
     UNION ALL
     
     SELECT CAST(NULL AS varchar(1)) AS to 
     , et.tx_hash
     , slice(et.trace_address, 1, cardinality(et.trace_address) - 1) AS trace
     , -et.gas_used AS gas_used
     , et.block_time
     , et.block_number
     FROM {{ source('ethereum','traces') }} et
     WHERE cardinality(et.trace_address) > 0
     AND et.block_time >= NOW() - interval '3' day
     {% if is_incremental() %}
     AND et.block_time >= date_trunc("day", NOW() - interval '1' week)
     {% endif %}
     ) etg
LEFT JOIN ethereum.transactions etc ON etc.block_time=etg.block_time
          AND etc.hash=etg.tx_hash
LEFT JOIN {{ source('prices', 'usd') }} pu ON pu.minute=date_trunc('minute', etg.block_time)
     AND pu.blockchain='ethereum'
     AND pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
     {% if is_incremental() %}
     AND pu.minute >= date_trunc("day", NOW() - interval '1' week)
     {% endif %}
GROUP BY etg.tx_hash, etg.trace, etc.gas_price, etg.block_time, etg.block_number
{% if is_incremental() %}