{{ config(
    schema = 'thorchain',
    alias = 'core_transfer_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'fact_transfer_events_id'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'core', 'transfer_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
    SELECT
        from_address,
        to_address,
        asset,
        amount_e8,
        event_id,
        block_timestamp,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_transfer_events') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.event_id',
        'a.from_address', 
        'a.to_address',
        'a.asset',
        'a.amount_e8'
    ]) }} AS fact_transfer_events_id,
    cast(date_trunc('day', b.block_timestamp) AS date) AS day,
    b.block_timestamp,  -- Include for compatibility
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.from_address,
    a.to_address,
    a.asset,
    a.amount_e8,
    a._inserted_timestamp,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_block') }} b
    ON a.block_timestamp = b.timestamp
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.block_timestamp') }}
{% endif %}
