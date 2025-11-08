{{ config(
    schema = 'thorchain_silver',
    alias = 'total_value_locked',
    materialized = 'table',
    file_format = 'delta',
    partition_by = ['day'],
    tags = ['thorchain', 'total_value_locked', 'silver']
) }}

WITH bond_type_day AS (
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        bond_type,
        (SUM(e8) / pow(10, 8)) AS rune_amount,
        MAX(a._inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('thorchain_silver_bond_events') }} AS a
    JOIN {{ ref('thorchain_silver_block_log') }} AS b
        ON a.block_timestamp = b.timestamp
    GROUP BY
        cast(date_trunc('day', b.block_timestamp) AS date),
        bond_type
),
bond_type_day_direction AS (
    SELECT
        day,
        bond_type,
        CASE
            WHEN bond_type IN (
            'bond_returned',
            'bond_cost'
            ) THEN -1
            ELSE 1
        END AS direction,
        rune_amount,
        rune_amount * direction AS abs_rune_amount,
        _inserted_timestamp
    FROM
        bond_type_day
),
total_value_bonded_tbl AS (
    SELECT
        day,
        SUM(abs_rune_amount) AS total_value_bonded,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        bond_type_day_direction
    GROUP BY
        day
),
total_pool_depth AS (
  SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        b.height AS block_id,
        pool_name,
        rune_e8,
        asset_e8,
        MAX(height) over (PARTITION BY pool_name, cast(date_trunc('day', b.block_timestamp) AS date)) AS max_block_id,
        a._inserted_timestamp
    FROM
        {{ ref('thorchain_silver_block_pool_depths') }} AS a
    JOIN {{ ref('thorchain_silver_block_log') }} AS b
        ON a.block_timestamp = b.timestamp
    WHERE LOWER(pool_name) NOT LIKE 'thor.%'
),
total_pool_depth_max AS (
    SELECT
        day,
        rune_e8 AS rune_depth,
        asset_e8 AS asset_depth,
        _inserted_timestamp
    FROM
        total_pool_depth
    WHERE
        block_id = max_block_id
),
total_value_pooled_tbl AS (
  SELECT
        day,
        SUM(rune_depth) * 2 / power(
            10,
            8
        ) AS total_value_pooled,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        total_pool_depth_max
    GROUP BY
        day
)
SELECT
    COALESCE(
        total_value_bonded_tbl.day,
        total_value_pooled_tbl.day
    ) AS DAY,
    COALESCE(
        total_value_pooled,
        0
    ) AS total_value_pooled,
    COALESCE(SUM(total_value_bonded) over (ORDER BY COALESCE(total_value_bonded_tbl.day, total_value_pooled_tbl.day) ASC), 0) AS total_value_bonded,
    COALESCE(
        total_value_pooled,
        0
    ) + SUM(COALESCE(total_value_bonded, 0)) over (
        ORDER BY
            COALESCE(
            total_value_bonded_tbl.day,
            total_value_pooled_tbl.day
            ) ASC
    ) AS total_value_locked,
    total_value_bonded_tbl._inserted_timestamp
FROM
  total_value_bonded_tbl full
JOIN total_value_pooled_tbl
  ON total_value_bonded_tbl.day = total_value_pooled_tbl.day