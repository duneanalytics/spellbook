{{ config(
    schema = 'gas_starknet',
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
     'starknet' as blockchain,
     date_trunc('day', block_time) AS block_date,
     CAST(date_trunc('month', block_time) AS DATE) AS block_month,
     block_number,
     block_time,
     txns.hash AS tx_hash,
     txns.sender_address AS tx_sender,
     cast(NULL AS VARBINARY) AS tx_receiver,
     CASE
        WHEN actual_fee_unit = 'WEI' THEN 'ETH'
        WHEN actual_fee_unit = 'FRI' THEN 'STRK'
        ELSE NULL
     END as native_token_symbol,
     cast(NULL as double) AS tx_amount_native,
     cast(NULL as double) AS tx_amount_usd,
     (actual_fee_amount/1e18) AS tx_fee_native,
     (actual_fee_amount/1e18) * p.price AS tx_fee_usd,
     cast(NULL as double) AS burned_native, -- Not applicable for L2s
     cast(NULL as double) AS burned_usd, -- Not applicable for L2s
     cast(NULL as VARBINARY) as validator, -- Not applicable for L2s
     cast(NULL as double) as gas_price_gwei,
     cast(NULL as double) as gas_price_usd,
     cast(NULL as bigint) as gas_used,
     cast(NULL as bigint) as gas_limit,
     cast(NULL as bigint) AS gas_usage_percent,
     type as transaction_type
FROM {{ source('starknet','transactions') }} txns
LEFT JOIN {{ source('prices','usd') }} p
    ON p.minute = date_trunc('minute', txns.block_time)
    AND p.blockchain IS NULL
    AND (
        (txns.actual_fee_unit = 'WEI' AND p.symbol = 'ETH')
        OR 
        (txns.actual_fee_unit = 'FRI' AND p.symbol = 'STRK')
    )
    {% if is_incremental() %}
    AND {{ incremental_predicate('p.minute') }}
    {% endif %}
{% if is_incremental() %}
WHERE {{ incremental_predicate('txns.block_time') }}
{% endif %}