{{ config(
    schema = 'thorchain',
    alias = 'defi_pool_block_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_pool_block_balances_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'pool_balances', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "defi",
                              "defi_pool_block_balances",
                              \'["krishhh"]\') }}'
) }}

WITH base AS (
    SELECT
        block_id,
        pool_name,
        rune_amount,
        rune_amount_usd,
        asset_amount,
        asset_amount_usd,
        synth_amount,
        synth_amount_usd,
        _unique_key,
        _inserted_timestamp,
        block_time
    FROM {{ ref('thorchain_silver_pool_block_balances') }}
    WHERE block_time >= current_date - interval '16' day
)

SELECT
    -- CRITICAL: Generate surrogate key (Trino equivalent of dbt_utils.generate_surrogate_key)
    to_hex(sha256(to_utf8(cast(a._unique_key as varchar)))) AS fact_pool_block_balances_id,
    
    -- CRITICAL: Always include partitioning columns first
    a.block_time,
    date(a.block_time) as block_date,
    date_trunc('month', a.block_time) as block_month,
    
    -- Block dimension reference (set directly - no JOIN needed)
    '-1' AS dim_block_id,
    
    -- Pool balance data
    a.pool_name,
    a.rune_amount,
    a.rune_amount_usd,
    a.asset_amount,
    a.asset_amount_usd,
    a.synth_amount,
    a.synth_amount_usd,
    
    -- Audit fields (Trino conversions)
    a._inserted_timestamp                AS source_inserted_timestamp,
    replace(cast(uuid() as varchar), '-', '') AS _audit_run_id,
    current_timestamp AS inserted_timestamp,  -- Trino equivalent of SYSDATE()
    current_timestamp AS modified_timestamp

FROM base a

{% if is_incremental() %}
WHERE {{ incremental_predicate('a.block_time') }}
{% endif %}
