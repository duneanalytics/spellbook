{{ config(
    schema = 'thorchain_silver',
    alias = 'pool_block_statistics',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['_unique_key'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags = ['thorchain', 'pool_statistics', 'silver']
) }}

-- Very complex daily statistics aggregation with multiple CTEs
WITH pool_depth AS (
    SELECT
        block_date,
        pool_name,
        rune_depth,
        asset_depth,
        synth_depth,
        rune_depth / nullif(asset_depth, 0) AS asset_price,
        block_month
    FROM (
        SELECT
            DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
            date_trunc('month', cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_month,
            b.height AS block_id,
            a.pool_name,
            a.rune_e8 AS rune_depth,
            a.asset_e8 AS asset_depth,
            a.synth_e8 AS synth_depth,
            MAX(b.height) OVER (
                PARTITION BY a.pool_name, DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))
            ) AS max_block_id
        FROM {{ ref('thorchain_silver_block_pool_depths') }} a
        JOIN {{ source('thorchain', 'block_log') }} b
            ON a.raw_block_timestamp = b.timestamp
        WHERE a.asset_e8 > 0
          AND cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
    )
    WHERE block_id = max_block_id
),

pool_status AS (
    SELECT
        block_date,
        asset AS pool_name,
        status
    FROM (
        SELECT
            DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
            a.asset,
            a.status,
            ROW_NUMBER() OVER (
                PARTITION BY a.asset, DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))
                ORDER BY cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) DESC, a.status
            ) AS rn
        FROM {{ ref('thorchain_silver_pool_events') }} a
        JOIN {{ source('thorchain', 'block_log') }} b
            ON a.block_time = cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)
        WHERE cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
    )
    WHERE rn = 1
),

add_liquidity_tbl AS (
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
        a.pool_name,
        COUNT(*) AS add_liquidity_count,
        SUM(a.rune_e8) AS add_rune_liquidity_volume,
        SUM(a.asset_e8) AS add_asset_liquidity_volume,
        SUM(a.stake_units) AS added_stake
    FROM {{ ref('thorchain_silver_stake_events') }} a
    JOIN {{ source('thorchain', 'block_log') }} b
        ON a.block_time = cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)
    WHERE cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
    GROUP BY
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)),
        a.pool_name
),

withdraw_tbl AS (
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
        a.pool AS pool_name,
        COUNT(*) AS withdraw_count,
        SUM(a.emit_rune_e8) AS withdraw_rune_volume,
        SUM(a.emit_asset_e8) AS withdraw_asset_volume,
        SUM(a.stake_units) AS withdrawn_stake,
        SUM(a.imp_loss_protection_e8) AS impermanent_loss_protection_paid
    FROM {{ ref('thorchain_silver_withdraw_events') }} a
    JOIN {{ source('thorchain', 'block_log') }} b
        ON a.raw_block_timestamp = b.timestamp
    WHERE cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
    GROUP BY
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)),
        a.pool
),

swap_total_tbl AS (
    SELECT
        block_date,
        pool as pool_name,
        SUM(volume) AS swap_volume
    FROM (
        SELECT
            DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
            a.pool,
            CASE
                WHEN a.to_asset = 'THOR.RUNE' THEN a.to_e8
                ELSE a.from_e8
            END AS volume
        FROM {{ ref('thorchain_silver_swap_events') }} a
        JOIN {{ source('thorchain', 'block_log') }} b
            ON a.raw_block_timestamp = b.timestamp
    )
    GROUP BY block_date, pool
),

swap_to_asset_tbl AS (
    SELECT
        block_date,
        pool as pool_name,
        SUM(liq_fee_in_rune_e8) AS to_asset_fees,
        SUM(from_e8) AS to_asset_volume,
        COUNT(*) AS to_asset_count,
        AVG(swap_slip_bp) AS to_asset_average_slip
    FROM (
        SELECT
            DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
            a.pool,
            CASE
                WHEN a.to_asset = 'THOR.RUNE' THEN 'to_rune'
                ELSE 'to_asset'
            END AS to_rune_asset,
            a.liq_fee_in_rune_e8,
            a.to_e8,
            a.from_e8,
            a.swap_slip_bp
        FROM {{ ref('thorchain_silver_swap_events') }} a
        JOIN {{ source('thorchain', 'block_log') }} b
            ON a.raw_block_timestamp = b.timestamp
    )
    WHERE to_rune_asset = 'to_asset'
    GROUP BY block_date, pool
),

swap_to_rune_tbl AS (
    SELECT
        block_date,
        pool as pool_name,
        SUM(liq_fee_in_rune_e8) AS to_rune_fees,
        SUM(to_e8) AS to_rune_volume,
        COUNT(*) AS to_rune_count,
        AVG(swap_slip_bp) AS to_rune_average_slip
    FROM (
        SELECT
            DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
            a.pool,
            CASE
                WHEN a.to_asset = 'THOR.RUNE' THEN 'to_rune'
                ELSE 'to_asset'
            END AS to_rune_asset,
            a.liq_fee_in_rune_e8,
            a.to_e8,
            a.from_e8,
            a.swap_slip_bp
        FROM {{ ref('thorchain_silver_swap_events') }} a
        JOIN {{ source('thorchain', 'block_log') }} b
            ON a.raw_block_timestamp = b.timestamp
    )
    WHERE to_rune_asset = 'to_rune'
    GROUP BY block_date, pool
),

average_slip_tbl AS (
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
        a.pool as pool_name,
        AVG(a.swap_slip_bp) AS average_slip
    FROM {{ ref('thorchain_silver_swap_events') }} a
    JOIN {{ source('thorchain', 'block_log') }} b
        ON a.raw_block_timestamp = b.timestamp
    GROUP BY
        a.pool,
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))
),

unique_swapper_tbl AS (
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
        a.pool as pool_name,
        COUNT(DISTINCT a.from_addr) AS unique_swapper_count
    FROM {{ ref('thorchain_silver_swap_events') }} a
    JOIN {{ source('thorchain', 'block_log') }} b
        ON a.raw_block_timestamp = b.timestamp
    GROUP BY
        a.pool,
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))
),

stake_amount AS (
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
        a.pool_name,
        SUM(a.stake_units) AS units
    FROM {{ ref('thorchain_silver_stake_events') }} a
    JOIN {{ source('thorchain', 'block_log') }} b
        ON a.block_time = cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)
    WHERE cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
    GROUP BY
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)),
        a.pool_name
),

-- FLIPSIDE ORIGINAL: Unstake tracking for unique member count
unstake_umc AS (
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
        a.from_addr AS address,
        a.pool AS pool_name,
        SUM(a.stake_units) AS unstake_liquidity_units
    FROM {{ ref('thorchain_silver_withdraw_events') }} a
    JOIN {{ source('thorchain', 'block_log') }} b
        ON a.raw_block_timestamp = b.timestamp
    WHERE cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
    GROUP BY
        a.from_addr,
        a.pool,
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))
),

-- FLIPSIDE ORIGINAL: Stake tracking for unique member count (split by rune/asset address)
stake_umc AS (
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
        a.rune_address AS address,
        a.pool_name,
        SUM(a.stake_units) AS liquidity_units
    FROM {{ ref('thorchain_silver_stake_events') }} a
    JOIN {{ source('thorchain', 'block_log') }} b
        ON a.block_time = cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)
    WHERE a.rune_address IS NOT NULL
        AND cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
    GROUP BY
        a.rune_address,
        a.pool_name,
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))
    
    UNION ALL
    
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
        a.asset_address AS address,
        a.pool_name,
        SUM(a.stake_units) AS liquidity_units
    FROM {{ ref('thorchain_silver_stake_events') }} a
    JOIN {{ source('thorchain', 'block_log') }} b
        ON a.block_time = cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)
    WHERE a.asset_address IS NOT NULL
        AND a.rune_address IS NULL
        AND cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
    GROUP BY
        a.asset_address,
        a.pool_name,
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))
),

-- FLIPSIDE ORIGINAL: Count unique members with net positive positions
unique_member_count AS (
    SELECT
        block_date,
        pool_name,
        COUNT(DISTINCT address) AS unique_member_count
    FROM (
        SELECT
            stake_umc.block_date,
            stake_umc.pool_name,
            stake_umc.address,
            stake_umc.liquidity_units,
            CASE
                WHEN unstake_umc.unstake_liquidity_units IS NOT NULL 
                    THEN unstake_umc.unstake_liquidity_units
                ELSE 0
            END AS unstake_liquidity_units
        FROM stake_umc
        LEFT JOIN unstake_umc
            ON stake_umc.address = unstake_umc.address
            AND stake_umc.pool_name = unstake_umc.pool_name
            AND stake_umc.block_date = unstake_umc.block_date
    )
    WHERE liquidity_units - unstake_liquidity_units > 0
    GROUP BY
        pool_name,
        block_date
),

asset_price_usd_tbl AS (
    SELECT
        date(p.block_time) AS block_date,
        p.symbol AS pool_name,
        p.price AS asset_price_usd
    FROM {{ ref('thorchain_silver_prices') }} p
    WHERE p.block_time >= current_date - interval '16' day
      AND p.symbol != 'RUNE'  -- Asset prices only
),

-- COMPLETE JOINED LOGIC - All original FlipsideCrypto CTEs now implemented
joined AS (
    SELECT
        pd.block_date,
        pd.block_month,
        COALESCE(alt.add_asset_liquidity_volume, 0) AS add_asset_liquidity_volume,
        COALESCE(alt.add_liquidity_count, 0) AS add_liquidity_count,
        COALESCE(alt.add_asset_liquidity_volume + alt.add_rune_liquidity_volume, 0) AS add_liquidity_volume,
        COALESCE(alt.add_rune_liquidity_volume, 0) AS add_rune_liquidity_volume,
        pd.pool_name AS asset,
        pd.asset_depth,
        COALESCE(pd.asset_price, 0) AS asset_price,
        COALESCE(apt.asset_price_usd, 0) AS asset_price_usd,
        COALESCE(ast.average_slip, 0) AS average_slip,
        COALESCE(wt.impermanent_loss_protection_paid, 0) AS impermanent_loss_protection_paid,
        COALESCE(pd.rune_depth, 0) AS rune_depth,
        COALESCE(pd.synth_depth, 0) AS synth_depth,
        COALESCE(ps.status, 'no status') AS status,
        COALESCE(trt.to_rune_count + tat.to_asset_count, 0) AS swap_count,
        COALESCE(stt.swap_volume, 0) AS swap_volume,
        COALESCE(tat.to_asset_average_slip, 0) AS to_asset_average_slip,
        COALESCE(tat.to_asset_count, 0) AS to_asset_count,
        COALESCE(tat.to_asset_fees, 0) AS to_asset_fees,
        COALESCE(tat.to_asset_volume, 0) AS to_asset_volume,
        COALESCE(trt.to_rune_average_slip, 0) AS to_rune_average_slip,
        COALESCE(trt.to_rune_count, 0) AS to_rune_count,
        COALESCE(trt.to_rune_fees, 0) AS to_rune_fees,
        COALESCE(trt.to_rune_volume, 0) AS to_rune_volume,
        COALESCE(trt.to_rune_fees + tat.to_asset_fees, 0) AS total_fees,
        COALESCE(umc.unique_member_count, 0) AS unique_member_count,
        COALESCE(ust.unique_swapper_count, 0) AS unique_swapper_count,
        COALESCE(sa.units, 0) AS units,
        COALESCE(wt.withdraw_asset_volume, 0) AS withdraw_asset_volume,
        COALESCE(wt.withdraw_count, 0) AS withdraw_count,
        COALESCE(wt.withdraw_rune_volume, 0) AS withdraw_rune_volume,
        COALESCE(wt.withdraw_rune_volume + wt.withdraw_asset_volume, 0) AS withdraw_volume,
        CAST(pd.asset_depth AS DOUBLE) * CAST(COALESCE(pd.rune_depth, 0) AS DOUBLE) AS depth_product,
        
        -- Daily stake changes (will calculate running total in outer SELECT)
        COALESCE(alt.added_stake, 0) - COALESCE(wt.withdrawn_stake, 0) AS daily_stake_change
        
    FROM pool_depth pd
    LEFT JOIN pool_status ps
        ON pd.pool_name = ps.pool_name AND pd.block_date = ps.block_date
    LEFT JOIN add_liquidity_tbl alt
        ON pd.pool_name = alt.pool_name AND pd.block_date = alt.block_date
    LEFT JOIN withdraw_tbl wt
        ON pd.pool_name = wt.pool_name AND pd.block_date = wt.block_date  -- FIXED: Use pool_name consistently
    LEFT JOIN swap_total_tbl stt
        ON pd.pool_name = stt.pool_name AND pd.block_date = stt.block_date
    LEFT JOIN swap_to_asset_tbl tat
        ON pd.pool_name = tat.pool_name AND pd.block_date = tat.block_date
    LEFT JOIN swap_to_rune_tbl trt
        ON pd.pool_name = trt.pool_name AND pd.block_date = trt.block_date
    LEFT JOIN unique_swapper_tbl ust
        ON pd.pool_name = ust.pool_name AND pd.block_date = ust.block_date
    LEFT JOIN stake_amount sa
        ON pd.pool_name = sa.pool_name AND pd.block_date = sa.block_date
    LEFT JOIN average_slip_tbl ast
        ON pd.pool_name = ast.pool_name AND pd.block_date = ast.block_date  
    LEFT JOIN asset_price_usd_tbl apt
        ON pd.pool_name = apt.pool_name AND pd.block_date = apt.block_date
    LEFT JOIN unique_member_count umc
        ON pd.pool_name = umc.pool_name AND pd.block_date = umc.block_date
)

-- Pre-calculate window functions to avoid nesting
, with_window_calcs AS (
    SELECT
        *,
        SUM(daily_stake_change) OVER (
            PARTITION BY asset
            ORDER BY block_date ASC
        ) AS total_stake,
        
        -- Synth units calculation
        CASE 
            WHEN synth_depth = 0 OR (CAST(asset_depth AS DOUBLE) * 2 - CAST(synth_depth AS DOUBLE)) = 0 THEN 0
            ELSE (SUM(daily_stake_change) OVER (PARTITION BY asset ORDER BY block_date ASC)) 
                 * CAST(synth_depth AS DOUBLE) 
                 / ((CAST(asset_depth AS DOUBLE) * 2) - CAST(synth_depth AS DOUBLE))
        END AS synth_units
    FROM joined
)

SELECT DISTINCT
    block_date,
    block_month,
    add_asset_liquidity_volume,
    add_liquidity_count, 
    add_liquidity_volume,
    add_rune_liquidity_volume,
    asset,
    asset_depth,
    asset_price,
    asset_price_usd,
    average_slip,
    impermanent_loss_protection_paid,
    rune_depth,
    synth_depth,
    status,
    swap_count,
    swap_volume,
    to_asset_average_slip,
    to_asset_count,
    to_asset_fees,
    to_asset_volume,
    to_rune_average_slip,
    to_rune_count,
    to_rune_fees,
    to_rune_volume,
    total_fees,
    unique_member_count,
    unique_swapper_count,
    units,
    withdraw_asset_volume,
    withdraw_count,
    withdraw_rune_volume,
    withdraw_volume,
    
    -- Window functions already calculated
    total_stake,
    depth_product,
    synth_units,
    
    -- Pool units = total_stake + synth_units
    total_stake + synth_units AS pool_units,
    
    -- Liquidity unit value index with safe SQRT
    CASE
        WHEN total_stake = 0 THEN 0
        WHEN depth_product < 0 THEN 0
        ELSE SQRT(depth_product) / (total_stake + synth_units)
    END AS liquidity_unit_value_index,
    
    -- Previous liquidity unit value index with LAG (no nested windows!)
    LAG(
        CASE
            WHEN total_stake = 0 THEN 0
            WHEN depth_product < 0 THEN 0
            ELSE SQRT(depth_product) / (total_stake + synth_units)
        END,
        1
    ) OVER (
        PARTITION BY asset
        ORDER BY block_date ASC
    ) AS prev_liquidity_unit_value_index,
    
    concat(
        cast(block_date as varchar),
        '-',
        asset
    ) AS _unique_key
FROM with_window_calcs
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_month') }}
{% endif %}
