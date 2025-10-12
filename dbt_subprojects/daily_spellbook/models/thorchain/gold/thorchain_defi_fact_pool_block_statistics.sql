{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_pool_block_statistics',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_pool_block_statistics_id'],
    partition_by = ['day_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags = ['thorchain', 'defi', 'pool_statistics', 'fact']
) }}

WITH base AS (
    SELECT
        day,
        add_asset_liquidity_volume,
        add_liquidity_count,
        add_liquidity_volume,
        add_rune_liquidity_volume,
        asset,
        asset_depth,
        asset_price,
        asset_price_usd,
        average_slip,
        impermanent_loss_protection_paid,
        rune_depth,
        synth_depth,
        status,
        swap_count,
        swap_volume,
        to_asset_average_slip,
        to_asset_count,
        to_asset_fees,
        to_asset_volume,
        to_rune_average_slip,
        to_rune_count,
        to_rune_fees,
        to_rune_volume,
        total_fees,
        unique_member_count,
        unique_swapper_count,
        units,
        withdraw_asset_volume,
        withdraw_count,
        withdraw_rune_volume,
        withdraw_volume,
        total_stake,
        depth_product,
        synth_units,
        pool_units,
        liquidity_unit_value_index,
        prev_liquidity_unit_value_index,
        _unique_key,
        day_month
    FROM {{ ref('thorchain_silver_pool_block_statistics') }}
    WHERE day >= current_date - interval '7' day
)

SELECT
    -- CRITICAL: Generate surrogate key (Trino equivalent of dbt_utils.generate_surrogate_key)
    to_hex(sha256(to_utf8(a._unique_key))) AS fact_pool_block_statistics_id,
    
    -- CRITICAL: Always include partitioning columns first
    a.day,
    a.day_month,
    
    -- Complete pool statistics data (all FlipsideCrypto columns)
    a.add_asset_liquidity_volume,
    a.add_liquidity_count,
    a.add_liquidity_volume,
    a.add_rune_liquidity_volume,
    a.asset,
    a.asset_depth,
    a.asset_price,
    a.asset_price_usd,
    a.average_slip,
    a.impermanent_loss_protection_paid,
    a.rune_depth,
    a.synth_depth,
    a.status,
    a.swap_count,
    a.swap_volume,
    a.to_asset_average_slip,
    a.to_asset_count,
    a.to_asset_fees,
    a.to_asset_volume,
    a.to_rune_average_slip,
    a.to_rune_count,
    a.to_rune_fees,
    a.to_rune_volume,
    a.total_fees,
    a.unique_member_count,
    a.unique_swapper_count,
    a.units,
    a.withdraw_asset_volume,
    a.withdraw_count,
    a.withdraw_rune_volume,
    a.withdraw_volume,
    a.total_stake,
    a.depth_product,
    a.synth_units,
    a.pool_units,
    a.liquidity_unit_value_index,
    a.prev_liquidity_unit_value_index,
    
    -- Audit fields (Trino conversions)
    cast(from_hex(replace(cast(uuid() as varchar), '-', '')) as varchar) AS _audit_run_id,  -- Trino equivalent of invocation_id
    current_timestamp AS inserted_timestamp,  -- Trino equivalent of SYSDATE()
    current_timestamp AS modified_timestamp

FROM base a

{% if is_incremental() %}
WHERE {{ incremental_predicate('a.block_date') }}
{% endif %}
