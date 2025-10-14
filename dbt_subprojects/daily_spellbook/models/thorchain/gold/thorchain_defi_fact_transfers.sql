{{ config(
    schema = 'thorchain_core',
    alias = 'fact_transfers',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_transfers_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'transfers', 'fact']
) }}

WITH base AS (
    SELECT
        block_id,
        from_address,
        to_address,
        asset,
        rune_amount,
        rune_amount_usd,
        _unique_key,
        block_time,
        block_timestamp,
        block_month,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_transfers') }}
    WHERE block_time >= current_date - interval '14' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['a._unique_key']) }} AS fact_transfers_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.from_address,
    a.to_address,
    a.asset,
    a.rune_amount,
    a.rune_amount_usd,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_dim_block') }} b
    ON a.block_id = b.block_id

