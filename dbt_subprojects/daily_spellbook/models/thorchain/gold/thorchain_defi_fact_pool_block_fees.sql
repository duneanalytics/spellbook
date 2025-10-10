{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_pool_block_fees',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_pool_block_fees_id'],
    partition_by = ['day_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'defi', 'pool_fees', 'fact']
) }}

WITH base AS (
    SELECT
        day,
        pool_name,
        rewards,
        total_liquidity_fees_rune,
        asset_liquidity_fees,
        rune_liquidity_fees,
        earnings,
        _unique_key,
        _inserted_timestamp,
        day_month
    FROM {{ ref('thorchain_silver_pool_block_fees') }}
    WHERE day >= current_date - interval '7' day
)

SELECT
    -- CRITICAL: Generate surrogate key (Trino equivalent of dbt_utils.generate_surrogate_key)
    to_hex(sha256(to_utf8(a._unique_key))) AS fact_pool_block_fees_id,
    
    -- CRITICAL: Always include partitioning columns first
    a.day,
    a.day_month,
    
    -- Pool fee data
    a.pool_name,
    a.rewards,
    a.total_liquidity_fees_rune,
    a.asset_liquidity_fees,
    a.rune_liquidity_fees,
    a.earnings,
    
    -- Audit fields (Trino conversions)
    a._inserted_timestamp,
    cast(from_hex(replace(cast(uuid() as varchar), '-', '')) as varchar) AS _audit_run_id,  -- Trino equivalent of invocation_id
    current_timestamp AS inserted_timestamp,  -- Trino equivalent of SYSDATE()
    current_timestamp AS modified_timestamp

FROM base a

{% if is_incremental() %}
WHERE {{ incremental_predicate('a.day') }}
{% endif %}
