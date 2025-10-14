{{ config(
    schema = 'thorchain_silver',
    alias = 'scheduled_outbound_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'scheduled_outbound_events', 'silver']
) }}

-- Deduplication using ROW_NUMBER (Trino doesn't support QUALIFY)
WITH deduplicated AS (
    SELECT
        chain,
        to_addr,
        asset,
        asset_e8,
        asset_decimals,
        gas_rate,
        memo,
        in_hash,
        out_hash,
        max_gas_amount,
        max_gas_decimals,
        max_gas_asset,
        module_name,
        vault_pub_key,
        event_id,
        block_timestamp,
        _updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY _updated_at DESC
        ) AS rn
    FROM {{ source('thorchain', 'scheduled_outbound_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '14' day
),

base AS (
    SELECT
        chain AS blockchain,
        to_addr AS to_address,
        asset,
        asset_e8,
        asset_e8 / POWER(10, 8) AS asset_amount,
        asset_decimals,
        gas_rate,
        memo,
        in_hash,
        out_hash,
        max_gas_amount,
        max_gas_decimals,
        max_gas_asset,
        module_name,
        vault_pub_key,
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
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}

