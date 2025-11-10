{{ config(
    schema = 'thorchain_silver',
    alias = 'daily_pool_stats',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', '_unique_key'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'daily_pool_stats', 'silver']
) }}

WITH daily_rune_price AS (
    SELECT
        pool_name,
        cast(date_trunc('day', block_timestamp) AS date) AS day,
        AVG(rune_usd) AS rune_usd,
        AVG(asset_usd) AS asset_usd
    FROM
        {{ ref('thorchain_silver_prices') }} as p
    {% if is_incremental() or true -%}
    WHERE {{ incremental_predicate('p.block_timestamp') }}
    {% endif -%}
    GROUP BY
        pool_name,
        cast(date_trunc('day', block_timestamp) AS date)
),
pool_fees AS (
    SELECT
        pbf.day,
        pbf.pool_name,
        rewards AS system_rewards,
        rewards * rune_usd AS system_rewards_usd,
        asset_liquidity_fees,
        asset_liquidity_fees * asset_usd AS asset_liquidity_fees_usd,
        rune_liquidity_fees,
        rune_liquidity_fees * rune_usd AS rune_liquidity_fees_usd
    FROM
        {{ ref('thorchain_silver_pool_block_fees') }} as pbf
    JOIN daily_rune_price drp
        ON pbf.day = drp.day
        AND pbf.pool_name = drp.pool_name
    {% if is_incremental() or true -%}
    WHERE {{ incremental_predicate('pbf.day') }}
    {% endif -%}
)
SELECT
    pbs.day,
    pbs.asset AS pool_name,
    COALESCE(
        system_rewards,
        0
    ) AS system_rewards,
    COALESCE(
        system_rewards_usd,
        0
    ) AS system_rewards_usd,
    COALESCE(asset_depth / pow(10, 8), 0) AS asset_liquidity,
    COALESCE(
        asset_price,
        0
    ) AS asset_price,
    COALESCE(
        asset_price_usd,
        0
    ) AS asset_price_usd,
    COALESCE(rune_depth / pow(10, 8), 0) AS rune_liquidity,
    COALESCE(asset_price_usd / NULLIF(rune_usd, 0), 0) AS rune_price,
    COALESCE(
        rune_usd,
        0
    ) AS rune_price_usd,
    COALESCE(
        add_liquidity_count,
        0
    ) AS add_liquidity_count,
    COALESCE(add_asset_liquidity_volume / pow(10, 8), 0) AS add_asset_liquidity,
    COALESCE(add_asset_liquidity_volume / pow(10, 8) * asset_usd, 0) AS add_asset_liquidity_usd,
    COALESCE(add_rune_liquidity_volume / pow(10, 8), 0) AS add_rune_liquidity,
    COALESCE(add_rune_liquidity_volume / pow(10, 8) * rune_usd, 0) AS add_rune_liquidity_usd,
    COALESCE(
        withdraw_count,
        0
    ) AS withdraw_count,
    COALESCE(withdraw_asset_volume / pow(10, 8), 0) AS withdraw_asset_liquidity,
    COALESCE(withdraw_asset_volume / pow(10, 8) * asset_usd, 0) AS withdraw_asset_liquidity_usd,
    COALESCE(withdraw_rune_volume / pow(10, 8), 0) AS withdraw_rune_liquidity,
    COALESCE(withdraw_rune_volume / pow(10, 8) * rune_usd, 0) AS withdraw_rune_liquidity_usd,
    COALESCE(impermanent_loss_protection_paid / pow(10, 8), 0) AS il_protection_paid,
    COALESCE(impermanent_loss_protection_paid / pow(10, 8) * rune_usd, 0) AS il_protection_paid_usd,
    COALESCE(
        average_slip,
        0
    ) AS average_slip,
    COALESCE(
        to_asset_average_slip,
        0
    ) AS to_asset_average_slip,
    COALESCE(
        to_rune_average_slip,
        0
    ) AS to_rune_average_slip,
    COALESCE(
        swap_count,
        0
    ) AS swap_count,
    COALESCE(
        to_asset_count,
        0
    ) AS to_asset_swap_count,
    COALESCE(
        to_rune_count,
        0
    ) AS to_rune_swap_count,
    COALESCE(swap_volume / pow(10, 8), 0) AS swap_volume_rune,
    COALESCE(swap_volume / pow(10, 8) * rune_usd, 0) AS swap_volume_rune_usd,
    COALESCE(to_asset_volume / pow(10, 8), 0) AS to_asset_swap_volume,
    COALESCE(to_rune_volume / pow(10, 8), 0) AS to_rune_swap_volume,
    COALESCE(totalfees / pow(10, 8), 0) AS total_swap_fees_rune,
    COALESCE(totalfees / pow(10, 8) * rune_usd, 0) AS total_swap_fees_usd,
    COALESCE(to_asset_fees / pow(10, 8), 0) AS total_asset_swap_fees,
    COALESCE(to_rune_fees / pow(10, 8), 0) AS total_asset_rune_fees,
    COALESCE(
        unique_member_count,
        0
    ) AS unique_member_count,
    COALESCE(
        unique_swapper_count,
        0
    ) AS unique_swapper_count,
    COALESCE(
        units,
        0
    ) AS liquidity_units,
    concat_ws(
        '-',
        cast(pbs.day as varchar),
        cast(pbs.asset as varchar)
    ) AS _unique_key
FROM
    {{ ref('thorchain_silver_pool_block_statistics') }} as pbs
LEFT JOIN daily_rune_price drp
    ON pbs.day = drp.day
    AND pbs.asset = drp.pool_name
LEFT JOIN pool_fees pf
    ON pbs.day = pf.day
    AND pbs.asset = pf.pool_name
{% if is_incremental() or true -%}
WHERE {{ incremental_predicate('pbs.day') }}
{% endif -%}