{{ config(
    schema = 'thorchain_silver',
    alias = 'pool_block_statistics',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'block_date', 'asset'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags = ['thorchain', 'pool_statistics', 'silver']
) }}

{% set lookback_days = 17 %}

-- Base CTE: Compute timestamp conversions once to avoid repeated calculations
WITH block_log_base AS (
    SELECT 
        timestamp AS raw_timestamp,
        height,
        DATE(cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
        CAST(date_trunc('month', cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp)) AS DATE) AS block_month,
        cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp) AS block_time
    FROM {{ ref('thorchain_silver_block_log') }}
    WHERE cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '{{ lookback_days }}' day
    {% if is_incremental() %}
        -- On incremental runs, only process new data plus a 3-day lookback window
        AND cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp) >= (
            SELECT COALESCE(MAX(block_date), current_date - interval '{{ lookback_days }}' day) - interval '3' day 
            FROM {{ this }}
        )
    {% endif %}
),

pool_depth AS (
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
            b.block_date,
            b.block_month,
            a.pool_name,
            a.rune_e8 AS rune_depth,
            a.asset_e8 AS asset_depth,
            a.synth_e8 AS synth_depth,
            ROW_NUMBER() OVER (
                PARTITION BY a.pool_name, b.block_date
                ORDER BY b.height DESC, a.rune_e8 DESC, a.asset_e8 DESC
            ) AS rn
        FROM {{ ref('thorchain_silver_block_pool_depths') }} a
        JOIN block_log_base b
            ON a.raw_block_timestamp = b.raw_timestamp
        WHERE a.asset_e8 > 0
    )
    WHERE rn = 1
),

pool_status AS (
    SELECT
        block_date,
        asset AS pool_name,
        status
    FROM (
        SELECT
            b.block_date,
            a.asset,
            a.status,
            ROW_NUMBER() OVER (
                PARTITION BY a.asset, b.block_date
                ORDER BY b.block_time DESC, a.status
            ) AS rn
        FROM {{ ref('thorchain_silver_pool_events') }} a
        JOIN block_log_base b
            ON a.block_time = b.block_time
    )
    WHERE rn = 1
),

add_liquidity_tbl AS (
    SELECT
        b.block_date,
        a.pool_name,
        COUNT(*) AS add_liquidity_count,
        SUM(a.rune_e8) AS add_rune_liquidity_volume,
        SUM(a.asset_e8) AS add_asset_liquidity_volume,
        SUM(a.stake_units) AS added_stake
    FROM {{ ref('thorchain_silver_stake_events') }} a
    JOIN block_log_base b
        ON a.block_time = b.block_time
    GROUP BY
        b.block_date,
        a.pool_name
),

withdraw_tbl AS (
    SELECT
        b.block_date,
        a.pool AS pool_name,
        COUNT(*) AS withdraw_count,
        SUM(a.emit_rune_e8) AS withdraw_rune_volume,
        SUM(a.emit_asset_e8) AS withdraw_asset_volume,
        SUM(a.stake_units) AS withdrawn_stake,
        SUM(a.imp_loss_protection_e8) AS impermanent_loss_protection_paid
    FROM {{ ref('thorchain_silver_withdraw_events') }} a
    JOIN block_log_base b
        ON a.raw_block_timestamp = b.raw_timestamp
    GROUP BY
        b.block_date,
        a.pool
),

swap_total_tbl AS (
    SELECT
        block_date,
        pool as pool_name,
        SUM(volume) AS swap_volume
    FROM (
        SELECT
            b.block_date,
            a.pool,
            CASE
                WHEN a.to_asset = 'THOR.RUNE' THEN a.to_e8
                ELSE a.from_e8
            END AS volume
        FROM {{ ref('thorchain_silver_swap_events') }} a
        JOIN block_log_base b
            ON a.raw_block_timestamp = b.raw_timestamp
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
            b.block_date,
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
        JOIN block_log_base b
            ON a.raw_block_timestamp = b.raw_timestamp
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
            b.block_date,
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
        JOIN block_log_base b
            ON a.raw_block_timestamp = b.raw_timestamp
    )
    WHERE to_rune_asset = 'to_rune'
    GROUP BY block_date, pool
),

average_slip_tbl AS (
    SELECT
        b.block_date,
        a.pool as pool_name,
        AVG(a.swap_slip_bp) AS average_slip
    FROM {{ ref('thorchain_silver_swap_events') }} a
    JOIN block_log_base b
        ON a.raw_block_timestamp = b.raw_timestamp
    GROUP BY
        b.block_date,
        a.pool
),

unique_swapper_tbl AS (
    SELECT
        b.block_date,
        a.pool as pool_name,
        COUNT(DISTINCT a.from_addr) AS unique_swapper_count
    FROM {{ ref('thorchain_silver_swap_events') }} a
    JOIN block_log_base b
        ON a.raw_block_timestamp = b.raw_timestamp
    GROUP BY
        b.block_date,
        a.pool
),

stake_amount AS (
    SELECT
        b.block_date,
        a.pool_name,
        SUM(a.stake_units) AS units
    FROM {{ ref('thorchain_silver_stake_events') }} a
    JOIN block_log_base b
        ON a.block_time = b.block_time
    GROUP BY
        b.block_date,
        a.pool_name
),

unstake_umc AS (
    SELECT
        b.block_date,
        a.from_addr AS address,
        a.pool AS pool_name,
        SUM(a.stake_units) AS unstake_liquidity_units
    FROM {{ ref('thorchain_silver_withdraw_events') }} a
    JOIN block_log_base b
        ON a.raw_block_timestamp = b.raw_timestamp
    GROUP BY
        b.block_date,
        a.from_addr,
        a.pool
),

stake_umc AS (
    SELECT
        b.block_date,
        a.rune_address AS address,
        a.pool_name,
        SUM(a.stake_units) AS liquidity_units
    FROM {{ ref('thorchain_silver_stake_events') }} a
    JOIN block_log_base b
        ON a.block_time = b.block_time
    WHERE a.rune_address IS NOT NULL
    GROUP BY
        b.block_date,
        a.rune_address,
        a.pool_name
    
    UNION ALL
    
    SELECT
        b.block_date,
        a.asset_address AS address,
        a.pool_name,
        SUM(a.stake_units) AS liquidity_units
    FROM {{ ref('thorchain_silver_stake_events') }} a
    JOIN block_log_base b
        ON a.block_time = b.block_time
    WHERE a.asset_address IS NOT NULL
        AND a.rune_address IS NULL
    GROUP BY
        b.block_date,
        a.asset_address,
        a.pool_name
),

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
            COALESCE(unstake_umc.unstake_liquidity_units, 0) AS unstake_liquidity_units
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
        block_date,
        pool_name,
        asset_price_usd
    FROM (
        SELECT
            date(p.block_time) AS block_date,
            p.symbol AS pool_name,
            p.asset_usd AS asset_price_usd,
            ROW_NUMBER() OVER (
                PARTITION BY p.symbol, date(p.block_time)
                ORDER BY p.block_id DESC, p.block_time DESC, p.asset_usd DESC
            ) AS rn
        FROM {{ ref('thorchain_silver_prices') }} p
        WHERE p.block_time >= current_date - interval '{{ lookback_days }}' day
        {% if is_incremental() %}
            AND date(p.block_time) >= (
                SELECT COALESCE(MAX(block_date), current_date - interval '{{ lookback_days }}' day) - interval '3' day 
                FROM {{ this }}
            )
        {% endif %}
    )
    WHERE rn = 1
),

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

, joined_with_history AS (
    SELECT * FROM joined
    
    {% if is_incremental() %}
    -- On incremental runs, retrieve the last known state for each asset before the current window
    -- This ensures cumulative window functions have the historical context they need
    UNION ALL
    SELECT
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
        daily_stake_change,
        depth_product
    FROM (
        SELECT
            asset,
            MAX(block_date) as block_date,
            MAX_BY(block_month, block_date) as block_month,
            MAX_BY(add_asset_liquidity_volume, block_date) as add_asset_liquidity_volume,
            MAX_BY(add_liquidity_count, block_date) as add_liquidity_count,
            MAX_BY(add_liquidity_volume, block_date) as add_liquidity_volume,
            MAX_BY(add_rune_liquidity_volume, block_date) as add_rune_liquidity_volume,
            MAX_BY(asset_depth, block_date) as asset_depth,
            MAX_BY(asset_price, block_date) as asset_price,
            MAX_BY(asset_price_usd, block_date) as asset_price_usd,
            MAX_BY(average_slip, block_date) as average_slip,
            MAX_BY(impermanent_loss_protection_paid, block_date) as impermanent_loss_protection_paid,
            MAX_BY(rune_depth, block_date) as rune_depth,
            MAX_BY(synth_depth, block_date) as synth_depth,
            MAX_BY(status, block_date) as status,
            MAX_BY(swap_count, block_date) as swap_count,
            MAX_BY(swap_volume, block_date) as swap_volume,
            MAX_BY(to_asset_average_slip, block_date) as to_asset_average_slip,
            MAX_BY(to_asset_count, block_date) as to_asset_count,
            MAX_BY(to_asset_fees, block_date) as to_asset_fees,
            MAX_BY(to_asset_volume, block_date) as to_asset_volume,
            MAX_BY(to_rune_average_slip, block_date) as to_rune_average_slip,
            MAX_BY(to_rune_count, block_date) as to_rune_count,
            MAX_BY(to_rune_fees, block_date) as to_rune_fees,
            MAX_BY(to_rune_volume, block_date) as to_rune_volume,
            MAX_BY(total_fees, block_date) as total_fees,
            MAX_BY(unique_member_count, block_date) as unique_member_count,
            MAX_BY(unique_swapper_count, block_date) as unique_swapper_count,
            MAX_BY(units, block_date) as units,
            MAX_BY(withdraw_asset_volume, block_date) as withdraw_asset_volume,
            MAX_BY(withdraw_count, block_date) as withdraw_count,
            MAX_BY(withdraw_rune_volume, block_date) as withdraw_rune_volume,
            MAX_BY(withdraw_volume, block_date) as withdraw_volume,
            MAX_BY(daily_stake_change, block_date) as daily_stake_change,
            MAX_BY(depth_product, block_date) as depth_product
        FROM {{ this }}
        WHERE block_date < (SELECT MIN(block_date) FROM joined)
        GROUP BY asset
    )
    {% endif %}
)

, with_calc AS (
SELECT
    pd.block_date,
    pd.block_month,
    add_asset_liquidity_volume,
    add_liquidity_count,
    add_liquidity_volume,
    add_rune_liquidity_volume,
    pd.asset,
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
    
    SUM(daily_stake_change) OVER (
        PARTITION BY pd.asset
        ORDER BY pd.block_date ASC
    ) AS total_stake,
    
    depth_product,
    
    CASE 
        WHEN synth_depth = 0 OR (CAST(asset_depth AS DOUBLE) * 2 - CAST(synth_depth AS DOUBLE)) = 0 THEN 0
        ELSE (SUM(daily_stake_change) OVER (PARTITION BY pd.asset ORDER BY pd.block_date ASC)) 
             * CAST(synth_depth AS DOUBLE) 
             / ((CAST(asset_depth AS DOUBLE) * 2) - CAST(synth_depth AS DOUBLE))
    END AS synth_units,
    
    (SUM(daily_stake_change) OVER (PARTITION BY pd.asset ORDER BY pd.block_date ASC)) + 
    (CASE 
        WHEN synth_depth = 0 OR (CAST(asset_depth AS DOUBLE) * 2 - CAST(synth_depth AS DOUBLE)) = 0 THEN 0
        ELSE (SUM(daily_stake_change) OVER (PARTITION BY pd.asset ORDER BY pd.block_date ASC)) 
             * CAST(synth_depth AS DOUBLE) 
             / ((CAST(asset_depth AS DOUBLE) * 2) - CAST(synth_depth AS DOUBLE))
    END) AS pool_units,
    
    CASE
        WHEN (SUM(daily_stake_change) OVER (PARTITION BY pd.asset ORDER BY pd.block_date ASC)) = 0 THEN 0
        WHEN depth_product < 0 THEN 0
        ELSE SQRT(depth_product) / (
            (SUM(daily_stake_change) OVER (PARTITION BY pd.asset ORDER BY pd.block_date ASC)) + 
            (CASE 
                WHEN synth_depth = 0 OR (CAST(asset_depth AS DOUBLE) * 2 - CAST(synth_depth AS DOUBLE)) = 0 THEN 0
                ELSE (SUM(daily_stake_change) OVER (PARTITION BY pd.asset ORDER BY pd.block_date ASC)) 
                     * CAST(synth_depth AS DOUBLE) 
                     / ((CAST(asset_depth AS DOUBLE) * 2) - CAST(synth_depth AS DOUBLE))
            END)
        )
    END AS liquidity_unit_value_index
    
FROM joined_with_history pd
)

SELECT
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
    total_stake,
    depth_product,
    synth_units,
    pool_units,
    liquidity_unit_value_index,
    LAG(liquidity_unit_value_index, 1) OVER (
        PARTITION BY asset
        ORDER BY block_date ASC
    ) AS prev_liquidity_unit_value_index,
    concat(
        cast(block_month as varchar),
        '-',
        cast(block_date as varchar),
        '-',
        asset
    ) AS _unique_key
FROM with_calc
{% if is_incremental() %}
-- Only return new dates for the merge; historical data was only included for window function context
WHERE {{ incremental_predicate('block_date') }}
{% endif %}
