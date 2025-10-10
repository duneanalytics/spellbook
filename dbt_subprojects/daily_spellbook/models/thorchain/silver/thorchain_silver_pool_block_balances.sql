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
with blk as (
    SELECT
        timestamp as raw_ts,
        cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp) as block_time,
        height
    FROM {{ source('thorchain', 'block_log') }}
),
base as (
    SELECT
        blk.block_time,
        blk.height AS block_id,
        bpd.pool_name,
        
        -- RUNE amounts and USD values
        COALESCE(cast(bpd.rune_e8 as double) / 1e8, 0.0) AS rune_amount,
        COALESCE(cast(bpd.rune_e8 as double) / 1e8 * COALESCE(rp.price, 0.0), 0.0) AS rune_amount_usd,
        
        -- Asset amounts and USD values  
        COALESCE(cast(bpd.asset_e8 as double) / 1e8, 0.0) AS asset_amount,
        COALESCE(cast(bpd.asset_e8 as double) / 1e8 * COALESCE(ap.price, 0.0), 0.0) AS asset_amount_usd,
        
        -- Synth amounts and USD values (use same asset price)
        COALESCE(cast(bpd.synth_e8 as double) / 1e8, 0.0) AS synth_amount,
        COALESCE(cast(bpd.synth_e8 as double) / 1e8 * COALESCE(ap.price, 0.0), 0.0) AS synth_amount_usd,
        
        -- Unique key generation
        concat(
            cast(bpd.raw_block_timestamp as varchar),
            '-',
            bpd.pool_name
        ) AS _unique_key,
        
        -- Timestamp conversions  
        date(blk.block_time) as block_date,
        date_trunc('month', blk.block_time) as block_month,
        date_trunc('hour', blk.block_time) as block_hour,
        
        bpd._inserted_timestamp

    FROM {{ ref('thorchain_silver_block_pool_depths') }} bpd
    JOIN blk ON bpd.raw_block_timestamp = blk.raw_ts
    -- Join RUNE prices using standard prices.usd pattern
    LEFT JOIN {{ source('prices', 'usd') }} rp
        ON rp.blockchain = 'thorchain'
        AND rp.symbol = 'RUNE'
        AND rp.minute = date_trunc('minute', blk.block_time)
    -- Join asset prices for pool assets using standard prices.usd pattern
    LEFT JOIN {{ source('prices', 'usd') }} ap
        ON ap.blockchain = 'thorchain'
        AND ap.symbol = bpd.pool_name  -- Use symbol instead of contract_address for Thorchain
        AND ap.minute = date_trunc('minute', blk.block_time)
    WHERE blk.block_time >= current_date - interval '7' day
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
    block_date,
    block_month,
    block_hour,
    _inserted_timestamp
FROM base
{% if is_incremental() %}
WHERE {{ incremental_predicate('base.block_time') }}
{% endif %}
