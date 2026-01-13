{{ config(
    schema = 'thorchain',
    alias = 'defi_loan_repayment_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'fact_loan_repayment_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'loan_repayment_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
    SELECT
        owner,
        collateral_asset,
        event_id,
        block_timestamp,
        collateral_withdrawn,
        debt_repaid,
        tx_id,
        block_time,
        block_month,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_loan_repayment_events') }}
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['a.event_id']) }} AS fact_loan_repayment_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.owner,
    a.collateral_asset,
    a.collateral_withdrawn,
    a.debt_repaid,
    a.tx_id,
    a.event_id,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_block') }} b
    ON a.block_timestamp = b.timestamp

