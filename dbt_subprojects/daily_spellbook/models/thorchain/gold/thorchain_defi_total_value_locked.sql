{{ config(
    schema = 'thorchain',
    alias = 'defi_total_value_locked',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_total_value_locked_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags = ['thorchain', 'defi', 'total_value_locked', 'fact', 'tvl'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "defi",
                              "defi_total_value_locked",
                              \'["krishhh"]\') }}'
) }}

WITH base AS (
    SELECT
        block_date,
        block_month,
        total_value_pooled,
        total_value_bonded,
        total_value_locked,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_total_value_locked') }}
    {% if is_incremental() %}
    WHERE block_date >= (
        SELECT MAX(block_date - INTERVAL '2' DAY)  -- counteract clock skew
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'block_date'
    ]) }} AS fact_total_value_locked_id,
    block_date AS day,  -- Alias for consistency with original naming
    block_date,
    block_month,
    total_value_pooled,
    total_value_bonded,
    total_value_locked,
    _inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base

