{{ config(
    schema = 'thorchain_silver',
    alias = 'pool_block_statistics',
    materialized = 'table',
    file_format = 'delta',
    partition_by = ['day'],
    tags = ['thorchain', 'pool_statistics', 'silver']
) }}

WITH pool_depth AS (
    SELECT
        day,
        pool_name,
        rune_e8 AS rune_depth,
        asset_e8 AS asset_depth,
        synth_e8 AS synth_depth,
        rune_e8 / nullif(asset_e8,0) AS asset_price
    FROM
    (
        SELECT
            cast(date_trunc('day', b.block_timestamp) AS date) AS day,
            b.height AS block_id,
            pool_name,
            rune_e8,
            synth_e8,
            asset_e8,
            MAX(b.height) over (PARTITION BY pool_name, cast(date_trunc('day', b.block_timestamp) AS date)) AS max_block_id
        FROM
            {{ ref("thorchain_silver_block_pool_depths") }} AS a
        JOIN {{ ref('thorchain_silver_block_log') }} AS b
            ON a.block_timestamp = b.timestamp
        WHERE
            asset_e8 > 0
    )
    WHERE
        block_id = max_block_id
),
pool_status AS (
    SELECT
        day,
        pool_name,
        status
    FROM
    (
        SELECT
            cast(date_trunc('day', b.block_timestamp) AS date) AS day,
            asset AS pool_name,
            status,
            ROW_NUMBER() over (
                PARTITION BY asset, cast(date_trunc('day', b.block_timestamp) AS date)
                ORDER BY b.block_timestamp DESC, status
            ) AS rn
        FROM
            {{ ref("thorchain_silver_pool_events") }} AS a
        JOIN {{ ref('thorchain_silver_block_log') }} AS b
            ON a.block_timestamp = b.timestamp
    )
    WHERE
        rn = 1
),
add_liquidity_tbl AS (
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        pool_name,
        COUNT(*) AS add_liquidity_count,
        SUM(rune_e8) AS add_rune_liquidity_volume,
        SUM(asset_e8) AS add_asset_liquidity_volume,
        SUM(stake_units) AS added_stake
    FROM
        {{ ref("thorchain_silver_stake_events") }} AS a
    JOIN {{ ref('thorchain_silver_block_log') }} AS b
        ON a.block_timestamp = b.timestamp
    GROUP BY
        cast(date_trunc('day', b.block_timestamp) AS date),
        pool_name
),
withdraw_tbl AS (
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        pool_name,
        COUNT(*) AS withdraw_count,
        SUM(emit_rune_e8) AS withdraw_rune_volume,
        SUM(emit_asset_e8) AS withdraw_asset_volume,
        SUM(stake_units) AS withdrawn_stake,
        SUM(imp_loss_protection_e8) AS impermanent_loss_protection_paid
    FROM
        {{ ref("thorchain_silver_withdraw_events") }} AS a
    JOIN {{ ref('thorchain_silver_block_log') }} AS b
        ON a.block_timestamp = b.timestamp
    GROUP BY
        cast(date_trunc('day', b.block_timestamp) AS date),
        pool_name
),
swap_total_tbl AS (
    SELECT
        day,
        pool_name,
        SUM(volume) AS swap_volume
    FROM
    (
        SELECT
            cast(date_trunc('day', b.block_timestamp) AS date) AS day,
            pool_name,
            CASE
                WHEN to_asset = 'THOR.RUNE' THEN to_e8
                ELSE from_e8
            END AS volume
        FROM
            {{ ref("thorchain_silver_swap_events") }} AS a
        JOIN {{ ref('thorchain_silver_block_log') }} AS b
            ON a.block_timestamp = b.timestamp
    )
    GROUP BY
        day,
        pool_name
),
swap_to_asset_tbl AS (
    SELECT
        day,
        pool_name,
        SUM(liq_fee_in_rune_e8) AS to_asset_fees,
        SUM(from_e8) AS to_asset_volume,
        COUNT(*) AS to_asset_count,
        AVG(swap_slip_bp) AS to_asset_average_slip
    FROM(
        SELECT
            cast(date_trunc('day', b.block_timestamp) AS date) AS day,
            pool_name,
            CASE
                WHEN to_asset = 'THOR.RUNE' THEN 'to_rune'
                ELSE 'to_asset'
            END AS to_tune_asset,
            liq_fee_in_rune_e8,
            to_e8,
            from_e8,
            swap_slip_bp,
            CASE
                WHEN to_asset = 'THOR.RUNE' THEN 0
                ELSE liq_fee_e8
            END AS asset_fee
        FROM
            {{ ref("thorchain_silver_swap_events") }} AS a
        JOIN {{ ref('thorchain_silver_block_log') }} AS b
            ON a.block_timestamp = b.timestamp
    )
    GROUP BY
        to_tune_asset,
        pool_name,
        day
    HAVING
        to_tune_asset = 'to_asset'
),
swap_to_rune_tbl AS (
    SELECT
        day,
        pool_name,
        SUM(liq_fee_in_rune_e8) AS to_rune_fees,
        SUM(to_e8) AS to_rune_volume,
        COUNT(*) AS to_rune_count,
        AVG(swap_slip_bp) AS to_rune_average_slip
    FROM(
        SELECT
            cast(date_trunc('day', b.block_timestamp) AS date) AS day,
            pool_name,
            CASE
                WHEN to_asset = 'THOR.RUNE' THEN 'to_rune'
                ELSE 'to_asset'
            END AS to_tune_asset,
            liq_fee_in_rune_e8,
            to_e8,
            from_e8,
            swap_slip_bp,
            CASE
                WHEN to_asset = 'THOR.RUNE' THEN 0
                ELSE liq_fee_e8
            END AS asset_fee
        FROM
            {{ ref("thorchain_silver_swap_events") }} AS a
        JOIN {{ ref('thorchain_silver_block_log') }} AS b
            ON a.block_timestamp = b.timestamp
    )
    GROUP BY
        to_tune_asset,
        pool_name,
        day
    HAVING
        to_tune_asset = 'to_rune'
),
average_slip_tbl AS (
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        pool_name,
        AVG(swap_slip_bp) AS average_slip
    FROM
    {{ ref("thorchain_silver_swap_events") }} AS a
    JOIN {{ ref('thorchain_silver_block_log') }} AS b
        ON a.block_timestamp = b.timestamp
    GROUP BY
        pool_name,
        cast(date_trunc('day', b.block_timestamp) AS date)
),
unique_swapper_tbl AS (
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        pool_name,
        COUNT(DISTINCT from_address) AS unique_swapper_count
    FROM
        {{ ref("thorchain_silver_swap_events") }} AS a
    JOIN {{ ref('thorchain_silver_block_log') }} AS b
        ON a.block_timestamp = b.timestamp
    GROUP BY
        pool_name,
        cast(date_trunc('day', b.block_timestamp) AS date)
),
stake_amount AS (
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        pool_name,
        SUM(stake_units) AS units
    FROM
        {{ ref("thorchain_silver_stake_events") }} AS a
    JOIN {{ ref('thorchain_silver_block_log') }} AS b
        ON a.block_timestamp = b.timestamp
    GROUP BY
        pool_name,
        cast(date_trunc('day', b.block_timestamp) AS date)
),
unstake_umc AS (
  SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        from_address AS address,
        pool_name,
        SUM(stake_units) AS unstake_liquidity_units
    FROM
        {{ ref("thorchain_silver_withdraw_events") }} AS a
    JOIN {{ ref('thorchain_silver_block_log') }} AS b
        ON a.block_timestamp = b.timestamp
    GROUP BY
        from_address,
        pool_name,
        cast(date_trunc('day', b.block_timestamp) AS date)
),
stake_umc AS (
  SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        rune_address AS address,
        pool_name,
        SUM(stake_units) AS liquidity_units
    FROM
        {{ ref("thorchain_silver_stake_events") }} AS a
    JOIN {{ ref('thorchain_silver_block_log') }} AS b
        ON a.block_timestamp = b.timestamp
    WHERE
        rune_address IS NOT NULL
    GROUP BY
        rune_address,
        pool_name,
        cast(date_trunc('day', b.block_timestamp) AS date)
    UNION ALL
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        asset_address AS address,
        pool_name,
        SUM(stake_units) AS liquidity_units
    FROM
        {{ ref("thorchain_silver_stake_events") }} AS a
    JOIN {{ ref('thorchain_silver_block_log') }} AS b
        ON a.block_timestamp = b.timestamp
    WHERE
        asset_address IS NOT NULL
        AND rune_address IS NULL
    GROUP BY
        asset_address,
        pool_name,
        cast(date_trunc('day', b.block_timestamp) AS date)
),
unique_member_count AS (
    SELECT
        day,
        pool_name,
        COUNT(DISTINCT address) AS unique_member_count
    FROM
    (
        SELECT
            stake_umc.day,
            stake_umc.pool_name,
            stake_umc.address,
            stake_umc.liquidity_units,
            CASE
                WHEN unstake_umc.unstake_liquidity_units IS NOT NULL THEN unstake_umc.unstake_liquidity_units
                ELSE 0
            END AS unstake_liquidity_units
        FROM
            stake_umc
        LEFT JOIN unstake_umc
            ON stake_umc.address = unstake_umc.address
            AND stake_umc.pool_name = unstake_umc.pool_name
    )
    WHERE
        liquidity_units - unstake_liquidity_units > 0
    GROUP BY
        pool_name,
        day
),
asset_price_usd_tbl AS (
    SELECT
        day,
        pool_name,
        asset_usd AS asset_price_usd
    FROM
    (
        SELECT
            cast(date_trunc('day', block_timestamp) AS date) AS day,
            block_id,
            MAX(block_id) over (PARTITION BY pool_name, cast(date_trunc('day', block_timestamp) AS date)) AS max_block_id,
            pool_name,
            asset_usd
        FROM
            {{ ref("thorchain_silver_prices") }}
    )
    WHERE
        block_id = max_block_id
),
joined AS (
    SELECT
        pool_depth.day AS day,
        COALESCE(
            add_asset_liquidity_volume,
            0
        ) AS add_asset_liquidity_volume,
        COALESCE(
            add_liquidity_count,
            0
        ) AS add_liquidity_count,
        COALESCE(
            (
            add_asset_liquidity_volume + add_rune_liquidity_volume
            ),
            0
        ) AS add_liquidity_volume,
        COALESCE(
            add_rune_liquidity_volume,
            0
        ) AS add_rune_liquidity_volume,
        pool_depth.pool_name AS asset,
        asset_depth,
        COALESCE(
            asset_price,
            0
        ) AS asset_price,
        COALESCE(
            asset_price_usd,
            0
        ) AS asset_price_usd,
        COALESCE(
            average_slip,
            0
        ) AS average_slip,
        COALESCE(
            impermanent_loss_protection_paid,
            0
        ) AS impermanent_loss_protection_paid,
        COALESCE(
            rune_depth,
            0
        ) AS rune_depth,
        COALESCE(
            synth_depth,
            0
        ) AS synth_depth,
        COALESCE(
            status,
            'no status'
        ) AS status,
        COALESCE((to_rune_count + to_asset_count), 0) AS swap_count,
        COALESCE(
            swap_volume,
            0
        ) AS swap_volume,
        COALESCE(
            to_asset_average_slip,
            0
        ) AS to_asset_average_slip,
        COALESCE(
            to_asset_count,
            0
        ) AS to_asset_count,
        COALESCE(
            to_asset_fees,
            0
        ) AS to_asset_fees,
        COALESCE(
            to_asset_volume,
            0
        ) AS to_asset_volume,
        COALESCE(
            to_rune_average_slip,
            0
        ) AS to_rune_average_slip,
        COALESCE(
            to_rune_count,
            0
        ) AS to_rune_count,
        COALESCE(
            to_rune_fees,
            0
        ) AS to_rune_fees,
        COALESCE(
            to_rune_volume,
            0
        ) AS to_rune_volume,
        COALESCE((to_rune_fees + to_asset_fees), 0) AS totalFees,
        COALESCE(
            unique_member_count,
            0
        ) AS unique_member_count,
        COALESCE(
            unique_swapper_count,
            0
        ) AS unique_swapper_count,
        COALESCE(
            units,
            0
        ) AS units,
        COALESCE(
            withdraw_asset_volume,
            0
        ) AS withdraw_asset_volume,
        COALESCE(
            withdraw_count,
            0
        ) AS withdraw_count,
        COALESCE(
            withdraw_rune_volume,
            0
        ) AS withdraw_rune_volume,
        COALESCE((withdraw_rune_volume + withdraw_asset_volume), 0) AS withdraw_volume,
        SUM(COALESCE(added_stake, 0) - COALESCE(withdrawn_stake, 0)) over (
            PARTITION BY pool_depth.pool_name
            ORDER BY
            pool_depth.day ASC
        ) AS total_stake,
        asset_depth * COALESCE(
            rune_depth,
            0
        ) AS depth_product,
        total_stake * synth_depth / ((asset_depth * 2) - synth_depth) AS synth_units,
        CASE
            WHEN total_stake = 0 THEN 0
            WHEN depth_product < 0 THEN 0
            ELSE SQRT(depth_product) / (
            total_stake + synth_units
            )
        END AS liquidity_unit_value_index
    FROM
        pool_depth
    LEFT JOIN pool_status
        ON pool_depth.pool_name = pool_status.pool_name
        AND pool_depth.day = pool_status.day
    LEFT JOIN add_liquidity_tbl
        ON pool_depth.pool_name = add_liquidity_tbl.pool_name
        AND pool_depth.day = add_liquidity_tbl.day
    LEFT JOIN withdraw_tbl
        ON pool_depth.pool_name = withdraw_tbl.pool_name
        AND pool_depth.day = withdraw_tbl.day
    LEFT JOIN swap_total_tbl
        ON pool_depth.pool_name = swap_total_tbl.pool_name
        AND pool_depth.day = swap_total_tbl.day
    LEFT JOIN swap_to_asset_tbl
        ON pool_depth.pool_name = swap_to_asset_tbl.pool_name
        AND pool_depth.day = swap_to_asset_tbl.day
    LEFT JOIN swap_to_rune_tbl
        ON pool_depth.pool_name = swap_to_rune_tbl.pool_name
        AND pool_depth.day = swap_to_rune_tbl.day
    LEFT JOIN unique_swapper_tbl
        ON pool_depth.pool_name = unique_swapper_tbl.pool_name
        AND pool_depth.day = unique_swapper_tbl.day
    LEFT JOIN stake_amount
        ON pool_depth.pool_name = stake_amount.pool_name
        AND pool_depth.day = stake_amount.day
    LEFT JOIN average_slip_tbl
        ON pool_depth.pool_name = average_slip_tbl.pool_name
        AND pool_depth.day = average_slip_tbl.day
    LEFT JOIN unique_member_count
        ON pool_depth.pool_name = unique_member_count.pool_name
        AND pool_depth.day = unique_member_count.day
    LEFT JOIN asset_price_usd_tbl
        ON pool_depth.pool_name = asset_price_usd_tbl.pool_name
        AND pool_depth.day = asset_price_usd_tbl.day
)
SELECT DISTINCT
    day,
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
    totalFees,
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
    total_stake + synth_units AS pool_units,
    liquidity_unit_value_index,
    LAG(liquidity_unit_value_index,1) over (PARTITION BY asset ORDER BY DAY ASC) AS prev_liquidity_unit_value_index,
    concat_ws(
        '-',
        cast(day as varchar),
        asset
    ) AS _unique_key
FROM
    joined