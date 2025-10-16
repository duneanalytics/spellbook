{{ config(
    schema = 'thorchain_silver',
    alias = 'swaps',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'swaps', 'curated']
) }}

-- Following Snowflake logic: only swap_events (no streaming swaps), join on block_id + pool_name
WITH swaps AS (
    SELECT 
        se.tx_hash,
        se.chain AS blockchain,
        se.from_addr AS from_address,
        se.to_addr AS to_address,
        se.from_asset,
        se.from_e8,
        se.to_asset,
        se.to_e8,
        se.memo,
        se.pool AS pool_name,
        se.to_e8_min_amount AS to_e8_min,
        se.swap_slip_bp,
        se.liq_fee_e8,
        se.liq_fee_in_rune_e8,
        se.direction AS _DIRECTION,
        se.event_id,
        se.streaming_count,
        se.streaming_quantity,
        se.block_time AS block_timestamp,
        bh.block_id,
        se.tx_type AS _TX_TYPE,
        COUNT(1) OVER (PARTITION BY se.tx_hash) AS n_tx,
        RANK() OVER (PARTITION BY se.tx_hash ORDER BY se.liq_fee_e8 ASC) AS rank_liq_fee,
        se.block_date,
        se.block_month
    FROM {{ ref('thorchain_silver_swap_events') }} se
    LEFT JOIN (
        SELECT DISTINCT
            CAST(from_unixtime(CAST(timestamp/1e9 AS bigint)) AS timestamp) AS block_time,
            height AS block_id
        FROM {{ source('thorchain','block_log') }}
        WHERE CAST(from_unixtime(CAST(timestamp/1e9 AS bigint)) AS timestamp) >= current_date - interval '16' day
    ) bh ON se.block_time = bh.block_time
    WHERE se.block_time >= current_date - interval '16' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('se.block_time') }}
    {% endif %}
)

-- Following Snowflake select logic exactly
SELECT 
    se.block_timestamp,
    se.block_id,
    se.tx_hash,
    se.blockchain,
    se.pool_name,
    se.from_address,
    
    -- Native to address logic (Snowflake)
    CASE
        WHEN se.n_tx > 1
            AND se.rank_liq_fee = 1
            AND LENGTH(SPLIT(se.memo, ':')[4]) = 43 THEN SPLIT(se.memo, ':')[4]
        WHEN se.n_tx > 1
            AND LOWER(SUBSTR(se.memo, 1, 1)) IN ('s', '=')
            AND LENGTH(COALESCE(SPLIT(se.memo, ':')[2], '')) = 0 THEN se.from_address
        ELSE SPLIT(se.memo, ':')[2]
    END AS native_to_address,
    
    se.to_address AS to_pool_address,
    
    -- Affiliate fields (Snowflake)
    CASE
        WHEN COALESCE(SPLIT(se.memo, ':')[4], '') = '' THEN NULL
        WHEN STRPOS(SPLIT(se.memo, ':')[4], '/') > 0 THEN 
            SPLIT(SPLIT(se.memo, ':')[4], '/')[0]
        ELSE SPLIT(se.memo, ':')[4]
    END AS affiliate_address,
    
    TRY_CAST(
        CASE
            WHEN COALESCE(SPLIT(se.memo, ':')[5], '') = '' THEN NULL
            WHEN STRPOS(SPLIT(se.memo, ':')[5], '/') > 0 THEN 
                SPLIT(SPLIT(se.memo, ':')[5], '/')[0]
            ELSE SPLIT(se.memo, ':')[5]
        END AS INTEGER
    ) AS affiliate_fee_basis_points,
    
    se.from_asset,
    se.to_asset,
    COALESCE(se.from_e8 / POWER(10, 8), 0) AS from_amount,
    COALESCE(se.to_e8 / POWER(10, 8), 0) AS to_amount,
    COALESCE(se.to_e8_min / POWER(10, 8), 0) AS min_to_amount,
    
    -- USD calculations (Snowflake logic)
    CASE
        WHEN se.from_asset = 'THOR.RUNE' THEN COALESCE(se.from_e8 * p.rune_usd / POWER(10, 8), 0)
        ELSE COALESCE(se.from_e8 * p.asset_usd / POWER(10, 8), 0)
    END AS from_amount_usd,
    
    CASE
        WHEN se.to_asset = 'THOR.RUNE' OR se.to_asset = 'BNB.RUNE-B1A' 
        THEN COALESCE(se.to_e8 * p.rune_usd / POWER(10, 8), 0)
        ELSE COALESCE(se.to_e8 * p.asset_usd / POWER(10, 8), 0)
    END AS to_amount_usd,
    
    p.rune_usd,
    p.asset_usd,
    
    CASE
        WHEN se.to_asset = 'THOR.RUNE' THEN COALESCE(se.to_e8_min * p.rune_usd / POWER(10, 8), 0)
        ELSE COALESCE(se.to_e8_min * p.asset_usd / POWER(10, 8), 0)
    END AS to_amount_min_usd,
    
    se.swap_slip_bp,
    COALESCE(se.liq_fee_in_rune_e8 / POWER(10, 8), 0) AS liq_fee_rune,
    COALESCE(se.liq_fee_in_rune_e8 / POWER(10, 8) * p.rune_usd, 0) AS liq_fee_rune_usd,
    
    CASE
        WHEN se.to_asset = 'THOR.RUNE' THEN COALESCE(se.liq_fee_e8 / POWER(10, 8), 0)
        ELSE COALESCE(se.liq_fee_e8 / POWER(10, 8), 0)
    END AS liq_fee_asset,
    
    CASE
        WHEN se.to_asset = 'THOR.RUNE' THEN COALESCE(se.liq_fee_e8 * p.rune_usd / POWER(10, 8), 0)
        ELSE COALESCE(se.liq_fee_e8 * p.asset_usd / POWER(10, 8), 0)
    END AS liq_fee_asset_usd,
    
    se.streaming_count,
    se.streaming_quantity,
    se._TX_TYPE,
    
    -- Spellbook fields
    se.block_date,
    se.block_month,
    se.event_id

FROM swaps se
LEFT JOIN {{ ref('thorchain_silver_prices') }} p
    ON se.block_id = p.block_id
    AND se.pool_name = p.pool_name
