{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_daily_pool_stats',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_daily_pool_stats_id'],
    partition_by = ['day_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags = ['thorchain', 'defi', 'daily_pool_stats', 'fact']
) }}

WITH base AS (
    SELECT
        day,
        pool_name,
        system_rewards,
        system_rewards_usd,
        asset_liquidity,
        asset_price,
        asset_price_usd,
        rune_liquidity,
        rune_price,
        rune_price_usd,
        add_liquidity_count,
        add_asset_liquidity,
        add_asset_liquidity_usd,
        add_rune_liquidity,
        add_rune_liquidity_usd,
        withdraw_count,
        withdraw_asset_liquidity,
        withdraw_asset_liquidity_usd,
        withdraw_rune_liquidity,
        withdraw_rune_liquidity_usd,
        il_protection_paid,
        il_protection_paid_usd,
        average_slip,
        to_asset_average_slip,
        to_rune_average_slip,
        swap_count,
        to_asset_swap_count,
        to_rune_swap_count,
        swap_volume_rune,
        swap_volume_rune_usd,
        to_asset_swap_volume,
        to_rune_swap_volume,
        total_swap_fees_rune,
        total_swap_fees_usd,
        total_asset_swap_fees,
        total_asset_rune_fees,
        unique_member_count,
        unique_swapper_count,
        liquidity_units,
        _unique_key,
        day_month
    FROM {{ ref('thorchain_silver_daily_pool_stats') }}
    WHERE day >= current_date - interval '7' day
)

SELECT
    -- CRITICAL: Generate surrogate key (Trino equivalent of dbt_utils.generate_surrogate_key)
    to_hex(sha256(to_utf8(concat(
        COALESCE(cast(a.day as varchar), ''),
        '|',
        COALESCE(a.pool_name, '')
    )))) AS fact_daily_pool_stats_id,
    
    -- CRITICAL: Always include partitioning columns first
    a.day,
    a.day_month,
    
    -- Pool statistics data (all the FlipsideCrypto columns)
    a.pool_name,
    a.system_rewards,
    a.system_rewards_usd,
    a.asset_liquidity,
    a.asset_price,
    a.asset_price_usd,
    a.rune_liquidity,
    a.rune_price,
    a.rune_price_usd,
    a.add_liquidity_count,
    a.add_asset_liquidity,
    a.add_asset_liquidity_usd,
    a.add_rune_liquidity,
    a.add_rune_liquidity_usd,
    a.withdraw_count,
    a.withdraw_asset_liquidity,
    a.withdraw_asset_liquidity_usd,
    a.withdraw_rune_liquidity,
    a.withdraw_rune_liquidity_usd,
    a.il_protection_paid,
    a.il_protection_paid_usd,
    a.average_slip,
    a.to_asset_average_slip,
    a.to_rune_average_slip,
    a.swap_count,
    a.to_asset_swap_count,
    a.to_rune_swap_count,
    a.swap_volume_rune,
    a.swap_volume_rune_usd,
    a.to_asset_swap_volume,
    a.to_rune_swap_volume,
    a.total_swap_fees_rune,
    a.total_swap_fees_usd,
    a.total_asset_swap_fees,
    a.total_asset_rune_fees,
    a.unique_member_count,
    a.unique_swapper_count,
    a.liquidity_units,
    
    -- Audit fields (Trino conversions)
    cast(from_hex(replace(cast(uuid() as varchar), '-', '')) as varchar) AS _audit_run_id,  -- Trino equivalent of invocation_id
    current_timestamp AS inserted_timestamp,  -- Trino equivalent of SYSDATE()
    current_timestamp AS modified_timestamp

FROM base a

{% if is_incremental() %}
WHERE {{ incremental_predicate('a.block_date') }}
{% endif %}
