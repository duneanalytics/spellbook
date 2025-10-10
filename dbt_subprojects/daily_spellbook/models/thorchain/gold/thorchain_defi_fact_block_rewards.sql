{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_block_rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_block_rewards_id'],
    partition_by = ['day_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'defi', 'block_rewards', 'fact']
) }}

WITH base AS (
    SELECT
        day,
        liquidity_fee,
        block_rewards,
        earnings,
        bonding_earnings,
        liquidity_earnings,
        avg_node_count,
        _inserted_timestamp,
        day_month
    FROM {{ ref('thorchain_silver_block_rewards') }}
    WHERE day >= current_date - interval '7' day
)

SELECT
    -- CRITICAL: Generate surrogate key (Trino equivalent of dbt_utils.generate_surrogate_key)
    to_hex(sha256(to_utf8(cast(a.day as varchar)))) AS fact_block_rewards_id,
    
    -- CRITICAL: Always include partitioning columns first
    a.day,
    a.day_month,
    
    -- Block rewards data
    a.liquidity_fee,
    a.block_rewards,
    a.earnings,
    a.bonding_earnings,
    a.liquidity_earnings,
    a.avg_node_count,
    
    -- Audit fields (Trino conversions)
    a._inserted_timestamp,
    cast(from_hex(replace(cast(uuid() as varchar), '-', '')) as varchar) AS _audit_run_id,  -- Trino equivalent of invocation_id
    current_timestamp AS inserted_timestamp,  -- Trino equivalent of SYSDATE()
    current_timestamp AS modified_timestamp

FROM base a

{% if is_incremental() %}
WHERE {{ incremental_predicate('a.day') }}
{% endif %}
