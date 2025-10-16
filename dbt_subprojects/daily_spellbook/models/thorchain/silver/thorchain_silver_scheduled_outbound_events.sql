{{ config(
    schema = 'thorchain_silver',
    alias = 'scheduled_outbound_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'scheduled_outbound_events', 'silver']
) }}

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
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '17' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
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
        array_join(max_gas_amount, ',') AS max_gas_amount,
        array_join(max_gas_decimals, ',') AS max_gas_decimals,
        array_join(max_gas_asset, ',') AS max_gas_asset,
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

