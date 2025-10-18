{{ config(
    schema = 'thorchain_silver',
    alias = 'gas_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'gas_events', 'silver']
) }}

WITH deduplicated AS (
    SELECT
        asset,
        asset_e8,
        rune_e8,
        tx_count,
        event_id,
        block_timestamp,
        _updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY _updated_at DESC
        ) AS rn
    FROM {{ source('thorchain', 'gas_events') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
),

base AS (
    SELECT
        asset,
        asset_e8,
        asset_e8 / POWER(10, 8) AS asset_amount,
        rune_e8,
        rune_e8 / POWER(10, 8) AS rune_amount,
        tx_count,
        event_id,
        block_timestamp,
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) AS block_time,
        DATE(cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
        date_trunc('month', cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)) AS block_month,
        date_trunc('hour', cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)) AS block_hour,
        current_timestamp AS _inserted_timestamp
    FROM deduplicated
    WHERE rn = 1
)

SELECT * FROM base

