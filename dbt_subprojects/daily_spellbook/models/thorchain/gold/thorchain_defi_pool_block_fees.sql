{{ config(
    schema = 'thorchain',
    alias = 'defi_pool_block_fees',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'fact_pool_block_fees_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags = ['thorchain', 'defi', 'pool_fees', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "defi",
                              "defi_pool_block_fees",
                              \'["krishhh"]\') }}'
) }}

WITH base AS (
    SELECT
        block_date,
        pool_name,
        rewards,
        total_liquidity_fees_rune,
        asset_liquidity_fees,
        rune_liquidity_fees,
        earnings,
        _unique_key,
        _inserted_timestamp,
        block_month
    FROM {{ ref('thorchain_silver_pool_block_fees') }}
    WHERE block_date >= current_date - interval '18' day
)

SELECT
    -- CRITICAL: Generate surrogate key (Trino equivalent of dbt_utils.generate_surrogate_key)
    to_hex(sha256(to_utf8(a._unique_key))) AS fact_pool_block_fees_id,
    
    -- CRITICAL: Always include partitioning columns first
    a.block_date,
    a.block_month,
    
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
WHERE {{ incremental_predicate('a.block_date') }}
{% endif %}
