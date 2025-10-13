{{ config(
    schema = 'thorchain_silver',
    alias = 'streaming_swap_details_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'swaps', 'streaming']
) }}

with base as (
    SELECT
    tx_id,
    "interval" as streaming_interval,
    quantity,
    count as stream_count,
    last_height,
    deposit_asset,
    deposit_e8 / 1e8 as deposit_amount,
    deposit_e8,
    in_asset,
    in_e8 / 1e8 as in_amount,
    in_e8,
    out_asset,
    out_e8 / 1e8 as out_amount,
    out_e8,
    failed_swaps,
    failed_swap_reasons,
    event_id,
    cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) as block_time,
    date(from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_date,
    date_trunc('month', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_month,
    date_trunc('hour', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_hour,
    block_timestamp as raw_block_timestamp,
    
    -- Deposit asset pricing fields - simplified approach using direct asset identifiers
    CASE 
        WHEN deposit_asset LIKE 'THOR.%' THEN cast(null as varbinary)
        -- For prices.usd table, we use the exact asset identifier as it appears
        ELSE cast(deposit_asset as varbinary)
    END as deposit_contract_address,
    
    -- In asset pricing fields - simplified approach using direct asset identifiers
    CASE 
        WHEN in_asset LIKE 'THOR.%' THEN cast(null as varbinary)
        -- For prices.usd table, we use the exact asset identifier as it appears
        ELSE cast(in_asset as varbinary)
    END as in_contract_address,
    
    -- Out asset pricing fields - simplified approach using direct asset identifiers
    CASE 
        WHEN out_asset LIKE 'THOR.%' THEN cast(null as varbinary)
        -- For prices.usd table, we use the exact asset identifier as it appears
        ELSE cast(out_asset as varbinary)
    END as out_contract_address

    FROM {{ source('thorchain', 'streaming_swap_details_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '15' day
)

SELECT * FROM base
{% if is_incremental() %}
WHERE {{ incremental_predicate('base.block_time') }}
{% endif %}
