{{ config(
    schema = 'thorchain_silver',
    alias = 'total_value_locked',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day'],
    partition_by = ['day_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'total_value_locked', 'silver']
) }}

-- Complex TVL calculation with bond events and pool depths
WITH bond_type_day AS (
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS day,
        a.bond_type,
        (SUM(a.e8) / pow(10, 8)) AS rune_amount,
        MAX(a._inserted_timestamp) AS _inserted_timestamp
    FROM {{ ref('thorchain_silver_bond_events') }} a  -- ✅ Now converted!
    JOIN {{ source('thorchain', 'block_log') }} b
        ON a.block_time = b.timestamp
    WHERE cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '7' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))') }}
    {% endif %}
    GROUP BY
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)),
        a.bond_type
),

bond_type_day_direction AS (
    SELECT
        day,
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
        day,
        SUM(abs_rune_amount) AS total_value_bonded,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM bond_type_day_direction
    GROUP BY day
),

total_pool_depth AS (
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS day,
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
        ON a.raw_block_time = b.timestamp
    WHERE LOWER(a.pool_name) NOT LIKE 'thor.%'
      AND cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '7' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))') }}
    {% endif %}
),

total_pool_depth_max AS (
    SELECT
        day,
        rune_e8 AS rune_depth,
        asset_e8 AS asset_depth,
        _inserted_timestamp
    FROM total_pool_depth
    WHERE block_id = max_block_id
),

total_value_pooled_tbl AS (
    SELECT
        day,
        SUM(rune_depth) * 2 / power(10, 8) AS total_value_pooled,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM total_pool_depth_max
    GROUP BY day
),

base AS (
    SELECT
        COALESCE(tvb.day, tvp.day) AS day,
        date_trunc('month', COALESCE(tvb.day, tvp.day)) as day_month,
        COALESCE(tvp.total_value_pooled, 0) AS total_value_pooled,
        
        -- Running total for bonded value (cumulative sum)
        SUM(COALESCE(tvb.total_value_bonded, 0)) OVER (
            ORDER BY COALESCE(tvb.day, tvp.day) ASC
        ) AS total_value_bonded,
        
        -- Total value locked = pooled + bonded
        COALESCE(tvp.total_value_pooled, 0) + 
        SUM(COALESCE(tvb.total_value_bonded, 0)) OVER (
            ORDER BY COALESCE(tvb.day, tvp.day) ASC
        ) AS total_value_locked,
        
        COALESCE(tvb._inserted_timestamp, tvp._inserted_timestamp) AS _inserted_timestamp
        
    FROM total_value_bonded_tbl tvb
    FULL OUTER JOIN total_value_pooled_tbl tvp  -- Trino equivalent of FULL JOIN
        ON tvb.day = tvp.day
)

SELECT * FROM base
{% if is_incremental() %}
WHERE {{ incremental_predicate('base.day') }}
{% endif %}
