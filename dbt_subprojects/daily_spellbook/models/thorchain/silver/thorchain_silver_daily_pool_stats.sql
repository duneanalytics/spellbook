{{ config(
    schema = 'thorchain_silver',
    alias = 'daily_pool_stats',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['_unique_key'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags = ['thorchain', 'daily_pool_stats', 'silver']
) }}

WITH daily_rune_price AS (
    SELECT
        symbol AS pool_name,
        date(block_time) AS block_date,
        AVG(rune_usd) AS rune_usd,
        AVG(asset_usd) AS asset_usd
    FROM {{ ref('thorchain_silver_prices') }} p
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('p.block_time') }}
    {% endif %}
    GROUP BY
        symbol,
        date(block_time)
),

pool_fees AS (
    SELECT
        pbf.block_date,
        pbf.pool_name,
        pbf.rewards AS system_rewards,
        pbf.rewards * COALESCE(drp.rune_usd, 0) AS system_rewards_usd,
        pbf.asset_liquidity_fees,
        pbf.asset_liquidity_fees * COALESCE(drp.asset_usd, 0) AS asset_liquidity_fees_usd,
        pbf.rune_liquidity_fees,
        pbf.rune_liquidity_fees * COALESCE(drp.rune_usd, 0) AS rune_liquidity_fees_usd
    FROM {{ ref('thorchain_silver_pool_block_fees') }} pbf
    LEFT JOIN daily_rune_price drp
        ON pbf.block_date = drp.block_date
        AND pbf.pool_name = drp.pool_name
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('pbf.block_date') }}
    {% endif %}
),

base AS (
    SELECT
        pbs.block_date,
        date_trunc('month', pbs.block_date) as block_month,
        pbs.asset AS pool_name,
        COALESCE(pf.system_rewards, 0) AS system_rewards,
        COALESCE(pf.system_rewards_usd, 0) AS system_rewards_usd,
        COALESCE(pbs.asset_depth / pow(10, 8), 0) AS asset_liquidity,
        COALESCE(pbs.asset_price, 0) AS asset_price,
        COALESCE(pbs.asset_price_usd, 0) AS asset_price_usd,
        COALESCE(pbs.rune_depth / pow(10, 8), 0) AS rune_liquidity,
        COALESCE(drp.asset_usd / NULLIF(drp.rune_usd, 0), 0) AS rune_price,
        COALESCE(drp.rune_usd, 0) AS rune_price_usd,
        COALESCE(pbs.add_liquidity_count, 0) AS add_liquidity_count,
        COALESCE(pbs.add_asset_liquidity_volume / pow(10, 8), 0) AS add_asset_liquidity,
        COALESCE(pbs.add_asset_liquidity_volume / pow(10, 8) * drp.asset_usd, 0) AS add_asset_liquidity_usd,
        COALESCE(pbs.add_rune_liquidity_volume / pow(10, 8), 0) AS add_rune_liquidity,
        COALESCE(pbs.add_rune_liquidity_volume / pow(10, 8) * drp.rune_usd, 0) AS add_rune_liquidity_usd,
        COALESCE(pbs.withdraw_count, 0) AS withdraw_count,
        COALESCE(pbs.withdraw_asset_volume / pow(10, 8), 0) AS withdraw_asset_liquidity,
        COALESCE(pbs.withdraw_asset_volume / pow(10, 8) * drp.asset_usd, 0) AS withdraw_asset_liquidity_usd,
        COALESCE(pbs.withdraw_rune_volume / pow(10, 8), 0) AS withdraw_rune_liquidity,
        COALESCE(pbs.withdraw_rune_volume / pow(10, 8) * drp.rune_usd, 0) AS withdraw_rune_liquidity_usd,
        COALESCE(pbs.impermanent_loss_protection_paid / pow(10, 8), 0) AS il_protection_paid,
        COALESCE(pbs.impermanent_loss_protection_paid / pow(10, 8) * drp.rune_usd, 0) AS il_protection_paid_usd,
        COALESCE(pbs.average_slip, 0) AS average_slip,
        COALESCE(pbs.to_asset_average_slip, 0) AS to_asset_average_slip,
        COALESCE(pbs.to_rune_average_slip, 0) AS to_rune_average_slip,
        COALESCE(pbs.swap_count, 0) AS swap_count,
        COALESCE(pbs.to_asset_count, 0) AS to_asset_swap_count,
        COALESCE(pbs.to_rune_count, 0) AS to_rune_swap_count,
        COALESCE(pbs.swap_volume / pow(10, 8), 0) AS swap_volume_rune,
        COALESCE(pbs.swap_volume / pow(10, 8) * drp.rune_usd, 0) AS swap_volume_rune_usd,
        COALESCE(pbs.to_asset_volume / pow(10, 8), 0) AS to_asset_swap_volume,
        COALESCE(pbs.to_rune_volume / pow(10, 8), 0) AS to_rune_swap_volume,
        COALESCE(pbs.total_fees / pow(10, 8), 0) AS total_swap_fees_rune,
        COALESCE(pbs.total_fees / pow(10, 8) * drp.rune_usd, 0) AS total_swap_fees_usd,
        COALESCE(pbs.to_asset_fees / pow(10, 8), 0) AS total_asset_swap_fees,
        COALESCE(pbs.to_rune_fees / pow(10, 8), 0) AS total_asset_rune_fees,
        COALESCE(pbs.unique_member_count, 0) AS unique_member_count,
        COALESCE(pbs.unique_swapper_count, 0) AS unique_swapper_count,
        COALESCE(pbs.units, 0) AS liquidity_units,
        concat(
            cast(pbs.block_date as varchar),
            '-',
            pbs.asset
        ) AS _unique_key
    FROM {{ ref('thorchain_silver_pool_block_statistics') }} pbs
    LEFT JOIN daily_rune_price drp
        ON pbs.block_date = drp.block_date
        AND pbs.asset = drp.pool_name
    LEFT JOIN pool_fees pf
        ON pbs.block_date = pf.block_date
        AND pbs.asset = pf.pool_name
)

SELECT * FROM base
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_date') }}
{% endif %}