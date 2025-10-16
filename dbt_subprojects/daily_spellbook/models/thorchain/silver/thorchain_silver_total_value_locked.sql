{{ config(
    schema = 'thorchain_silver',
    alias = 'total_value_locked',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags = ['thorchain', 'total_value_locked', 'silver']
) }}

WITH bond_type_day AS (
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
        a.bond_type,
        (SUM(a.e8) / pow(10, 8)) AS rune_amount,
        MAX(a._inserted_timestamp) AS _inserted_timestamp
    FROM {{ ref('thorchain_silver_bond_events') }} a
    JOIN {{ source('thorchain', 'block_log') }} b
        ON a.block_time = cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)
    WHERE cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('a.block_time') }}
    {% endif %}
    GROUP BY
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)),
        a.bond_type
),

bond_type_day_direction AS (
    SELECT
        block_date,
        bond_type,
        CASE
            WHEN bond_type IN ('bond_returned', 'bond_cost') THEN -1
            ELSE 1
        END AS direction,
        rune_amount,
        rune_amount * 
        CASE
            WHEN bond_type IN ('bond_returned', 'bond_cost') THEN -1
            ELSE 1
        END AS abs_rune_amount,
        _inserted_timestamp
    FROM bond_type_day
),

total_value_bonded_tbl AS (
    SELECT
        block_date,
        SUM(abs_rune_amount) AS total_value_bonded,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM bond_type_day_direction
    GROUP BY block_date
),

total_pool_depth AS (
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
        b.height AS block_id,
        a.pool_name,
        a.rune_e8,
        a.asset_e8,
        MAX(b.height) OVER (
            PARTITION BY a.pool_name, DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))
        ) AS max_block_id,
        a._inserted_timestamp
    FROM {{ ref('thorchain_silver_block_pool_depths') }} a
    JOIN {{ source('thorchain', 'block_log') }} b
        ON a.raw_block_timestamp = b.timestamp
    WHERE LOWER(a.pool_name) NOT LIKE 'thor.%'
      AND cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('a.block_time') }}
    {% endif %}
),

total_pool_depth_max AS (
    SELECT
        block_date,
        rune_e8 AS rune_depth,
        asset_e8 AS asset_depth,
        _inserted_timestamp
    FROM total_pool_depth
    WHERE block_id = max_block_id
),

total_value_pooled_tbl AS (
    SELECT
        block_date,
        SUM(rune_depth) * 2 / power(10, 8) AS total_value_pooled,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM total_pool_depth_max
    GROUP BY block_date
),

base AS (
    SELECT
        COALESCE(tvb.block_date, tvp.block_date) AS block_date,
        date_trunc('month', COALESCE(tvb.block_date, tvp.block_date)) as block_month,
        COALESCE(tvp.total_value_pooled, 0) AS total_value_pooled,
        
        SUM(COALESCE(tvb.total_value_bonded, 0)) OVER (
            ORDER BY COALESCE(tvb.block_date, tvp.block_date) ASC
        ) AS total_value_bonded,
        
        COALESCE(tvp.total_value_pooled, 0) + 
        SUM(COALESCE(tvb.total_value_bonded, 0)) OVER (
            ORDER BY COALESCE(tvb.block_date, tvp.block_date) ASC
        ) AS total_value_locked,
        
        COALESCE(tvb._inserted_timestamp, tvp._inserted_timestamp) AS _inserted_timestamp
        
    FROM total_value_bonded_tbl tvb
    FULL OUTER JOIN total_value_pooled_tbl tvp
        ON tvb.block_date = tvp.block_date
)

SELECT * FROM base
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_date') }}
{% endif %}