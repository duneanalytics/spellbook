{{ config(
    schema = 'thorchain_silver',
    alias = 'transfer_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'event_id', 'from_address', 'to_address', 'asset', 'amount_e8', 'block_timestamp'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'transfer_events', 'silver']
) }}

WITH deduplicated AS (
    SELECT
        from_addr,
        to_addr,
        asset,
        amount_e8,
        event_id,
        block_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY event_id, from_addr, to_addr, asset, amount_e8, block_timestamp
            ORDER BY _updated_at DESC
        ) AS rn
    FROM {{ source('thorchain', 'transfer_events') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
),

base AS (
    SELECT
        from_addr AS from_address,
        to_addr AS to_address,
        asset,
        amount_e8,
        amount_e8 / POWER(10, 8) AS amount,
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

