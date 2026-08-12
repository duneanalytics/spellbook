{{ config(
    schema = 'thorchain_silver',
    alias = 'pool_block_statistics',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'asset'],
    partition_by = ['day'],
    incremental_predicates = ['DBT_INTERNAL_DEST.day = DBT_INTERNAL_SOURCE.day'],
    pre_hook = "{{ set_trino_session_property(true, 'distinct_aggregations_strategy', 'single_step') }}",
    tags = ['thorchain', 'pool_statistics', 'silver']
) }}

-- ci-stamp: 1
WITH
{% if is_incremental() -%}
target_watermark AS (
    SELECT
        cast(MAX(day) AS timestamp) - interval '{{ var("DBT_ENV_INCREMENTAL_TIME") }}' {{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }} AS processed_after
    FROM {{ this }}
),
changed_withdrawals AS (
    SELECT
        date(from_unixtime(cast(w.block_timestamp / 1e9 AS bigint))) AS day,
        w.pool_name,
        w.from_address AS address
    FROM {{ ref("thorchain_silver_withdraw_events") }} AS w
    CROSS JOIN target_watermark AS tw
    WHERE w._inserted_timestamp >= tw.processed_after
),
changed_liquidity_days AS (
    SELECT
        s.block_date AS day
    FROM {{ ref("thorchain_silver_stake_events") }} AS s
    CROSS JOIN target_watermark AS tw
    WHERE s._ingested_timestamp >= tw.processed_after
    UNION ALL
    SELECT
        day
    FROM changed_withdrawals
    UNION ALL
    SELECT
        MIN(s.block_date) AS day
    FROM {{ ref("thorchain_silver_stake_events") }} AS s
    INNER JOIN changed_withdrawals AS w
        ON s.pool_name = w.pool_name
        AND COALESCE(s.rune_address, s.asset_address) = w.address
),
{% endif -%}
incremental_bounds AS (
    SELECT
        {% if is_incremental() -%}
        LEAST(
            cast(date_trunc('{{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }}', now() - interval '{{ var("DBT_ENV_INCREMENTAL_TIME") }}' {{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }}) AS date),
            COALESCE(
                (SELECT MIN(day) FROM changed_liquidity_days),
                cast(date_trunc('{{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }}', now() - interval '{{ var("DBT_ENV_INCREMENTAL_TIME") }}' {{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }}) AS date)
            )
        ) AS rebuild_start
        {% else -%}
        date '2021-04-11' AS rebuild_start
        {% endif -%}
),
pool_depth AS (
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
        CROSS JOIN incremental_bounds AS ib
        WHERE
            asset_e8 > 0
            AND b.block_date >= ib.rebuild_start
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
        CROSS JOIN incremental_bounds AS ib
        WHERE b.block_date >= ib.rebuild_start
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
    CROSS JOIN incremental_bounds AS ib
    WHERE b.block_date >= ib.rebuild_start
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
    CROSS JOIN incremental_bounds AS ib
    WHERE b.block_date >= ib.rebuild_start
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
        CROSS JOIN incremental_bounds AS ib
        WHERE b.block_date >= ib.rebuild_start
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
        CROSS JOIN incremental_bounds AS ib
        WHERE b.block_date >= ib.rebuild_start
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
        CROSS JOIN incremental_bounds AS ib
        WHERE b.block_date >= ib.rebuild_start
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
    CROSS JOIN incremental_bounds AS ib
    WHERE b.block_date >= ib.rebuild_start
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
    CROSS JOIN incremental_bounds AS ib
    WHERE b.block_date >= ib.rebuild_start
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
    CROSS JOIN incremental_bounds AS ib
    WHERE b.block_date >= ib.rebuild_start
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
    CROSS JOIN incremental_bounds AS ib
    WHERE
        rune_address IS NOT NULL
        AND b.block_date >= ib.rebuild_start
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
    CROSS JOIN incremental_bounds AS ib
    WHERE
        asset_address IS NOT NULL
        AND rune_address IS NULL
        AND b.block_date >= ib.rebuild_start
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
        CROSS JOIN incremental_bounds AS ib
        WHERE block_date >= ib.rebuild_start
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
        added_stake,
        withdrawn_stake
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
),
joined_deduplicated AS (
    SELECT DISTINCT
        *
    FROM joined
)
{% if is_incremental() -%}
, prior_state AS (
    SELECT
        t.asset,
        MAX_BY(t.total_stake, t.day) AS total_stake,
        MAX_BY(t.liquidity_unit_value_index, t.day) AS liquidity_unit_value_index,
        MAX(t.day) AS day
    FROM {{ this }} AS t
    CROSS JOIN incremental_bounds AS ib
    WHERE t.day < ib.rebuild_start
    GROUP BY t.asset
)
{% endif -%}
, total_stake AS (
    SELECT
        j.*,
        {% if is_incremental() -%}
        COALESCE(p.total_stake, 0) +
        {% endif -%}
        SUM(COALESCE(j.added_stake, 0) - COALESCE(j.withdrawn_stake, 0)) OVER (
            PARTITION BY j.asset
            ORDER BY j.day ASC
        ) AS total_stake,
        CAST(j.asset_depth AS double) * CAST(COALESCE(
            j.rune_depth,
            0
        ) AS double) AS depth_product
    FROM joined_deduplicated AS j
    {% if is_incremental() -%}
    LEFT JOIN prior_state AS p ON j.asset = p.asset
    {% endif -%}
),
synth_units AS (
    SELECT
        *,
        CAST(total_stake AS double) * CAST(synth_depth AS double) / ((CAST(asset_depth AS double) * CAST(2 AS double)) - CAST(synth_depth AS double)) AS synth_units
    FROM total_stake
),
final AS (
    SELECT
        *,
        CASE
            WHEN total_stake = 0 THEN 0
            WHEN depth_product < 0 THEN 0
            ELSE SQRT(CAST(depth_product AS double)) / (
                CAST(total_stake AS double) + CAST(synth_units AS double)
            )
        END AS liquidity_unit_value_index
    FROM synth_units
),
index_history AS (
    {% if is_incremental() -%}
    SELECT
        p.day,
        p.asset,
        p.liquidity_unit_value_index
    FROM prior_state AS p
    UNION ALL
    {% endif -%}
    SELECT
        f.day,
        f.asset,
        f.liquidity_unit_value_index
    FROM final AS f
),
lagged_indexes AS (
    SELECT
        day,
        asset,
        LAG(liquidity_unit_value_index, 1) OVER (
            PARTITION BY asset
            ORDER BY day ASC
        ) AS prev_liquidity_unit_value_index
    FROM index_history
)
SELECT
    f.day,
    f.add_asset_liquidity_volume,
    f.add_liquidity_count,
    f.add_liquidity_volume,
    f.add_rune_liquidity_volume,
    f.asset,
    f.asset_depth,
    f.asset_price,
    f.asset_price_usd,
    f.average_slip,
    f.impermanent_loss_protection_paid,
    f.rune_depth,
    f.status,
    f.swap_count,
    f.swap_volume,
    f.to_asset_average_slip,
    f.to_asset_count,
    f.to_asset_fees,
    f.to_asset_volume,
    f.to_rune_average_slip,
    f.to_rune_count,
    f.to_rune_fees,
    f.to_rune_volume,
    f.totalFees,
    f.unique_member_count,
    f.unique_swapper_count,
    f.units,
    f.withdraw_asset_volume,
    f.withdraw_count,
    f.withdraw_rune_volume,
    f.withdraw_volume,
    f.total_stake,
    f.depth_product,
    f.synth_units,
    f.total_stake + f.synth_units AS pool_units,
    f.liquidity_unit_value_index,
    i.prev_liquidity_unit_value_index,
    concat_ws(
        '-',
        cast(f.day AS varchar),
        f.asset
    ) AS _unique_key
FROM final AS f
INNER JOIN lagged_indexes AS i
    ON f.day = i.day
    AND f.asset = i.asset
