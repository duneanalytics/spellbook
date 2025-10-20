{{ config(
    schema = 'thorchain',
    alias = 'core_wasm_contracts_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_wasm_contracts_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'core', 'wasm_contracts_events', 'fact', 'smart_contracts'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "core",
                              "core_wasm_contracts_events",
                              \'["krishhh"]\') }}'
) }}

-- Deduplication and gold layer combined (no silver layer needed)
WITH deduplicated AS (
    SELECT
        tx_id,
        contract_address,
        contract_type,
        sender,
        attributes,
        event_id,
        block_timestamp,
        COALESCE(_updated_at, _ingested_at) AS row_ts,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY COALESCE(_updated_at, _ingested_at) DESC
        ) AS rn
    FROM {{ source('thorchain', 'wasm_contracts_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
),

base AS (
    SELECT
        tx_id,
        contract_address,
        contract_type,
        sender,
        attributes,
        event_id,
        block_timestamp,
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) AS block_time,
        date_trunc('month', cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)) AS block_month,
        COALESCE(row_ts, cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)) AS _inserted_timestamp
    FROM deduplicated
    WHERE rn = 1
      {% if is_incremental() %}
        AND {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
      {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.tx_id',
        'a.event_id',
        'a.contract_address',
        'a.sender'
    ]) }} AS fact_wasm_contracts_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.tx_id,
    a.contract_address,
    a.contract_type,
    a.sender,
    a.attributes,
    a.event_id,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_block') }} b
    ON a.block_timestamp = b.timestamp

