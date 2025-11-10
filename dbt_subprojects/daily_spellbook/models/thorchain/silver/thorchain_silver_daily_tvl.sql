{{ config(
    schema = 'thorchain_silver',
    alias = 'daily_tvl',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'daily_tvl', 'silver']
) }}

WITH max_daily_block AS (
    SELECT
        MAX(block_id) AS block_id,
        cast(date_trunc('day', block_timestamp) AS date) AS day
    FROM
        {{ ref('thorchain_silver_prices') }} as p
    {% if is_incremental() or true -%}
    WHERE {{ incremental_predicate('p.block_timestamp') }}
    {% endif -%}
    GROUP BY
    cast(date_trunc('day', block_timestamp) AS date)
),
daily_rune_price AS (
    SELECT
        p.block_id,
        mdb.day,
        AVG(rune_usd) AS rune_usd
    FROM
        {{ ref('thorchain_silver_prices') }} as p
    JOIN max_daily_block as mdb
        ON p.block_id = mdb.block_id
    {% if is_incremental() or true -%}
    WHERE {{ incremental_predicate('p.block_timestamp') }}
    {% endif -%}
    GROUP BY
        mdb.day,
        p.block_id
)
SELECT
    br.day,
    total_value_pooled AS total_value_pooled,
    total_value_pooled * rune_usd AS total_value_pooled_usd,
    total_value_bonded AS total_value_bonded,
    total_value_bonded * rune_usd AS total_value_bonded_usd,
    total_value_locked AS total_value_locked,
    total_value_locked * rune_usd AS total_value_locked_usd
FROM
    {{ ref('thorchain_silver_total_value_locked') }} as br
JOIN daily_rune_price drp
    ON br.day = drp.day