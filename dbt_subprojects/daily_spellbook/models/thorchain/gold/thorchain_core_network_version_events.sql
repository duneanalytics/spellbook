{{ config(
    schema = 'thorchain',
    alias = 'core_network_version_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_network_version_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'core', 'network_version_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "core",
                              "core_network_version_events",
                              \'["krishhh"]\') }}'
) }}

-- Deduplication and gold layer combined (no silver layer needed)
WITH deduplicated AS (
    SELECT
        version,
        event_id,
        block_timestamp,
        _updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM {{ source('thorchain', 'network_version_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
),

base AS (
    SELECT
        version,
        event_id,
        block_timestamp,
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) AS block_time,
        date_trunc('month', cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)) AS block_month,
        _updated_at AS _inserted_timestamp
    FROM deduplicated
    WHERE rn = 1
      {% if is_incremental() %}
        AND {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
      {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['a.event_id']) }} AS fact_network_version_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.version,
    a.event_id,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_block') }} b
    ON a.block_timestamp = b.timestamp

