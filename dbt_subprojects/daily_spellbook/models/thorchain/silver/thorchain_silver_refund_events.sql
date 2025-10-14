{{ config(
    schema = 'thorchain_silver',
    alias = 'refund_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id', 'tx_id', 'blockchain', 'from_address', 'to_address', 'asset', 'asset_2nd', 'memo', 'code', 'reason', 'block_timestamp'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'refund_events', 'silver']
) }}

-- Deduplication using ROW_NUMBER (Trino doesn't support QUALIFY)
WITH deduplicated AS (
    SELECT
        tx,
        chain,
        from_addr,
        to_addr,
        asset,
        asset_e8,
        asset_2nd,
        asset_2nd_e8,
        memo,
        code,
        reason,
        event_id,
        block_timestamp,
        _tx_type,
        _updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY event_id, tx, chain, from_addr, to_addr, asset, asset_2nd, memo, code, reason, block_timestamp
            ORDER BY _updated_at DESC
        ) AS rn
    FROM {{ source('thorchain', 'refund_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '14' day
),

base AS (
    SELECT
        tx AS tx_id,
        chain AS blockchain,
        from_addr AS from_address,
        to_addr AS to_address,
        asset,
        asset_e8,
        asset_e8 / POWER(10, 8) AS asset_amount,
        asset_2nd,
        asset_2nd_e8,
        asset_2nd_e8 / POWER(10, 8) AS asset_2nd_amount,
        memo,
        code,
        reason,
        event_id,
        block_timestamp,
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) AS block_time,
        DATE(cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)) AS block_date,
        date_trunc('month', cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)) AS block_month,
        date_trunc('hour', cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)) AS block_hour,
        _tx_type,
        current_timestamp AS _inserted_timestamp
    FROM deduplicated
    WHERE rn = 1
)

SELECT * FROM base
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}

