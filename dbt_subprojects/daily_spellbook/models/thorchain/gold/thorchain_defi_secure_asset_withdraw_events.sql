{{ config(
    schema = 'thorchain',
    alias = 'defi_secure_asset_withdraw_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'secure_asset_withdraw_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "defi",
                              "defi_secure_asset_withdraw_events",
                              \'["krishhh"]\') }}'
) }}

-- Deduplication and gold layer combined (no silver layer needed)
WITH deduplicated AS (
    SELECT
        amount_e8,
        asset,
        asset_address,
        rune_address,
        tx_id,
        event_id,
        block_timestamp,
        
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY block_timestamp DESC
        ) AS rn
    FROM {{ source('thorchain', 'secure_asset_withdraw_events') }}
    {% if not is_incremental() %}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '18' day
    {% endif %}
),

base AS (
    SELECT
        amount_e8,
        asset,
        asset_address,
        rune_address,
        tx_id,
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
        'a.amount_e8',
        'a.asset',
        'a.asset_address',
        'a.rune_address',
        'a.tx_id',
        'a.event_id',
        'a.block_timestamp'
    ]) }} AS fact_secure_asset_withdraw_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.amount_e8,
    a.asset,
    a.asset_address,
    a.rune_address,
    a.tx_id,
    a.event_id,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_block') }} b
    ON a.block_timestamp = b.timestamp

