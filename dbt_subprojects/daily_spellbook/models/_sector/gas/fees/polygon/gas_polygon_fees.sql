{{ config(
    schema = 'gas_polygon',
    alias = 'fees',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy='merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['tx_hash','block_number']
    )
}}

SELECT
     'polygon' as blockchain,
     date_trunc('day', block_time) AS block_date,
     CAST(date_trunc('month', block_time) AS DATE) AS block_month,
     block_number,
     block_time,
     txns.hash AS tx_hash,
     txns."from" AS tx_sender,
     txns.to AS tx_receiver,
     'MATIC' as native_token_symbol,
     value/1e18 AS tx_amount_native,
     value/1e18 * p.price AS tx_amount_usd,
     -- This is just a guess to compile.
     (cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double)) as tx_fee_native,
     (cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double)) * p.price AS tx_fee_usd,
     -- TODO: Determine how polygon handles these fields.
     cast(NULL as double) AS burned_native,
     cast(NULL as double) AS burned_usd,
     cast(NULL as VARBINARY) as validator,
     -- TODO: end todo.
     txns.gas_price/1e9 as gas_price_gwei,
     txns.gas_price/1e18 * p.price as gas_price_usd,
     txns.gas_used as gas_used,
     txns.gas_limit as gas_limit,
     CASE
        WHEN txns.gas_limit = 0 THEN NULL
        WHEN txns.gas_limit != 0 THEN txns.gas_used / txns.gas_limit * 100
     END AS gas_usage_percent,
     type as transaction_type

FROM {{ source('polygon','transactions') }} txns
JOIN {{ source('polygon','blocks') }} blocks ON blocks.number = txns.block_number
{% if is_incremental() %}
AND {{ incremental_predicate('block_time') }}
AND {{ incremental_predicate('blocks.time') }}
{% endif %}
LEFT JOIN {{ source('prices','usd') }} p ON p.minute = date_trunc('minute', block_time)
AND p.blockchain = 'polygon'
AND p.symbol = 'MATIC'
{% if is_incremental() %}
AND {{ incremental_predicate('p.minute') }}
WHERE {{ incremental_predicate('block_time') }}
AND {{ incremental_predicate('blocks.time') }}
AND {{ incremental_predicate('p.minute') }}
{% endif %}