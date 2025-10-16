{{ config(
    schema = 'thorchain_silver',
    alias = 'prices',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'block_id', 'pool_name'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'prices']
) }}


WITH blocks AS (
    SELECT
        bpd.pool_name,
        bpd.asset_e8,
        bpd.rune_e8,
        bpd.raw_block_timestamp,
        CAST(from_unixtime(CAST(bpd.raw_block_timestamp / 1e9 AS bigint)) AS timestamp) AS block_time,
        DATE(from_unixtime(CAST(bpd.raw_block_timestamp / 1e9 AS bigint))) AS block_date,
        date_trunc('month', from_unixtime(CAST(bpd.raw_block_timestamp / 1e9 AS bigint))) AS block_month,
        bl.height AS block_id
    FROM {{ ref('thorchain_silver_block_pool_depths') }} bpd
    JOIN {{ source('thorchain','block_log') }} bl
        ON bpd.raw_block_timestamp = bl.timestamp
    WHERE CAST(from_unixtime(CAST(bpd.raw_block_timestamp / 1e9 AS bigint)) AS timestamp) >= current_date - interval '17' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('CAST(from_unixtime(CAST(bpd.raw_block_timestamp / 1e9 AS bigint)) AS timestamp)') }}
    {% endif %}
),

rune_price AS (
    SELECT
        rp.block_time,
        rp.rune_price_usd AS rune_usd,
        bl.height AS block_id
    FROM {{ ref('thorchain_silver_rune_price') }} rp
    JOIN {{ source('thorchain','block_log') }} bl
        ON CAST(from_unixtime(CAST(rp.raw_block_timestamp / 1e9 AS bigint)) AS timestamp) = CAST(from_unixtime(CAST(bl.timestamp / 1e9 AS bigint)) AS timestamp)
    WHERE rp.block_time >= current_date - interval '17' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('rp.block_time') }}
    {% endif %}
)

SELECT DISTINCT
    b.block_id,
    b.block_time,
    b.pool_name,
    
    COALESCE(CAST(b.rune_e8 AS DOUBLE) / NULLIF(CAST(b.asset_e8 AS DOUBLE), 0), 0) AS price_rune_asset,
    COALESCE(CAST(b.asset_e8 AS DOUBLE) / NULLIF(CAST(b.rune_e8 AS DOUBLE), 0), 0) AS price_asset_rune,
    
    COALESCE(rp.rune_usd * (CAST(b.rune_e8 AS DOUBLE) / NULLIF(CAST(b.asset_e8 AS DOUBLE), 0)), 0) AS asset_usd,
    COALESCE(rp.rune_usd, 0) AS rune_usd,
    
    b.block_date,
    b.block_month,
    
    CASE 
        WHEN b.pool_name LIKE '%.%' THEN SPLIT(b.pool_name, '.')[2]
        ELSE b.pool_name
    END AS symbol,
    
    CAST(b.pool_name AS varbinary) AS contract_address,
    
    'thorchain' AS blockchain

FROM blocks b
JOIN rune_price rp
    ON b.block_id = rp.block_id
WHERE b.rune_e8 > 0
  AND b.asset_e8 > 0
