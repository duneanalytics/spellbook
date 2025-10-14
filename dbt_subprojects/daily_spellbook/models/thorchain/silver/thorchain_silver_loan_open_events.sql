{{ config(
    schema = 'thorchain_silver',
    alias = 'loan_open_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'loan_open_events', 'silver']
) }}

-- Deduplication using ROW_NUMBER (Trino doesn't support QUALIFY)
WITH deduplicated AS (
    SELECT
        owner,
        collateralization_ratio,
        collateral_asset,
        target_asset,
        event_id,
        collateral_deposited,
        debt_issued,
        tx_id,
        block_timestamp,
        _updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY _updated_at DESC
        ) AS rn
    FROM {{ source('thorchain', 'loan_open_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '14' day
),

base AS (
    SELECT
        owner,
        collateralization_ratio,
        collateral_asset,
        target_asset,
        event_id,
        collateral_deposited,
        debt_issued,
        tx_id,
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
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}

