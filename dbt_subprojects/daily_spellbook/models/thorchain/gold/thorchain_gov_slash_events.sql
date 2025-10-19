{{ config(
    schema = 'thorchain',
    alias = 'gov_slash_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_slash_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'gov', 'slash_events', 'fact', 'governance'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "governance",
                              "gov_slash_events",
                              \'["krishhh"]\') }}'
) }}

WITH deduplicated AS (
    SELECT
        pool AS pool_name,
        asset,
        asset_e8,
        event_id,
        block_timestamp,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY event_id, pool, asset
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM {{ source('thorchain', 'slash_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
),

base AS (
    SELECT
        pool_name,
        asset,
        asset_e8,
        event_id,
        block_timestamp,
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) AS block_time,
        date_trunc('month', cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)) AS block_month,
        current_timestamp AS _inserted_timestamp
    FROM deduplicated
    WHERE rn = 1
      {% if is_incremental() %}
        AND {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
      {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.event_id',
        'a.pool_name',
        'a.asset'
    ]) }} AS fact_slash_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.pool_name,
    a.asset,
    a.asset_e8,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_dim_block') }} b
    ON a.block_timestamp = b.timestamp

