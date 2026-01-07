{{ config(
    schema = 'thorchain',
    alias = 'core_thorname_change_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'fact_thorname_change_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'core', 'thorname_change_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

-- Deduplication and gold layer combined (no silver layer needed)
WITH deduplicated AS (
    SELECT
        owner,
        chain,
        address,
        expire,
        name,
        fund_amount_e8,
        registration_fee_e8,
        event_id,
        block_timestamp,
        
        ROW_NUMBER() OVER (
            PARTITION BY event_id, block_timestamp, owner, chain, address, expire, name
            ORDER BY block_timestamp DESC
        ) AS rn
    FROM {{ source('thorchain', 'thorname_change_events') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
),

base AS (
    SELECT
        owner,
        chain,
        address,
        expire,
        name,
        fund_amount_e8,
        registration_fee_e8,
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
        'a.block_timestamp',
        'a.owner',
        'a.chain',
        'a.address',
        'a.expire',
        'a.name'
    ]) }} AS fact_thorname_change_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.owner,
    a.chain,
    a.address,
    a.expire,
    a.name,
    a.fund_amount_e8,
    a.registration_fee_e8,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_block') }} b
    ON a.block_timestamp = b.timestamp

