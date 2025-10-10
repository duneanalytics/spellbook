{{ config(
    schema = 'thorchain_silver',
    alias = 'pool_block_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['_unique_key'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'pool_balances', 'silver']
) }}

-- CRITICAL: Use CTE pattern to avoid column resolution issues
with base as (
    SELECT
        cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) as block_time,
        b.height AS block_id,
        bpd.pool_name,
        
        -- RUNE amounts and USD values
        COALESCE(bpd.rune_e8 / pow(10, 8), 0) AS rune_amount,
        COALESCE(bpd.rune_e8 / pow(10, 8) * COALESCE(rp.price, 0), 0) AS rune_amount_usd,
        
        -- Asset amounts and USD values  
        COALESCE(bpd.asset_e8 / pow(10, 8), 0) AS asset_amount,
        COALESCE(bpd.asset_e8 / pow(10, 8) * COALESCE(ap.price, 0), 0) AS asset_amount_usd,
        
        -- Synth amounts and USD values (use same asset price)
        COALESCE(bpd.synth_e8 / pow(10, 8), 0) AS synth_amount,
        COALESCE(bpd.synth_e8 / pow(10, 8) * COALESCE(ap.price, 0), 0) AS synth_amount_usd,
        
        -- Unique key generation
        concat(
            cast(bpd.raw_block_timestamp as varchar),
            '-',
            bpd.pool_name
        ) AS _unique_key,
        
        -- Timestamp conversions  
        date(from_unixtime(cast(b.timestamp / 1e9 as bigint))) as block_date,
        date_trunc('month', from_unixtime(cast(b.timestamp / 1e9 as bigint))) as block_month,
        date_trunc('hour', from_unixtime(cast(b.timestamp / 1e9 as bigint))) as block_hour,
        
        bpd._inserted_timestamp

    FROM {{ ref('thorchain_silver_block_pool_depths') }} bpd
    JOIN {{ source('thorchain', 'block_log') }} b
        ON bpd.raw_block_timestamp = b.timestamp
    -- Join RUNE prices using standard prices.usd pattern
    LEFT JOIN {{ source('prices', 'usd') }} rp
        ON rp.blockchain = 'thorchain'
        AND rp.symbol = 'RUNE'
        AND rp.minute = date_trunc('minute', cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))
    -- Join asset prices for pool assets using standard prices.usd pattern
    LEFT JOIN {{ source('prices', 'usd') }} ap
        ON ap.blockchain = 'thorchain'
        AND ap.contract_address = cast(bpd.pool_name as varbinary)  -- Thorchain uses pool_name as asset identifier
        AND ap.minute = date_trunc('minute', cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))
    WHERE cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '7' day
)

SELECT 
    block_time,
    block_id,
    pool_name,
    rune_amount,
    rune_amount_usd,
    asset_amount,
    asset_amount_usd,
    synth_amount,
    synth_amount_usd,
    _unique_key,
    block_time,
    block_date,
    block_month,
    block_hour,
    _inserted_timestamp
FROM base
{% if is_incremental() %}
WHERE {{ incremental_predicate('base.block_time') }}
{% endif %}
