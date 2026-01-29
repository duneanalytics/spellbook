{{ config(
    schema = 'thorchain',
    alias = 'defi_switch_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'fact_switch_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'switch_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

-- Deduplication and gold layer combined (no silver layer needed)
WITH deduplicated AS (
    SELECT
        tx AS tx_id,
        from_addr AS from_address,
        to_addr AS to_address,
        burn_asset,
        burn_e8,
        mint_e8,
        event_id,
        block_timestamp,
        
        ROW_NUMBER() OVER (
            PARTITION BY tx, from_addr, to_addr, burn_asset, burn_e8, mint_e8, block_timestamp
            ORDER BY block_timestamp DESC
        ) AS rn
    FROM {{ source('thorchain', 'switch_events') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
),

base AS (
    SELECT
        tx_id,
        from_address,
        to_address,
        burn_asset,
        burn_e8,
        mint_e8,
        event_id,
        block_timestamp,
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) AS block_time,
        date_trunc('month', cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)) AS block_month,
        current_timestamp AS _inserted_timestamp
    FROM deduplicated
    WHERE rn = 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.event_id',
        'a.tx_id',
        'a.from_address',
        'a.to_address',
        'a.burn_asset',
        'a.burn_e8',
        'a.mint_e8'
    ]) }} AS fact_switch_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.tx_id,
    a.from_address,
    a.to_address,
    a.burn_asset,
    a.burn_e8,
    a.mint_e8,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_block') }} b
    ON a.block_timestamp = b.timestamp

