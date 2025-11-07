{{ config(
    schema = 'thorchain_silver',
    alias = 'prices',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'block_id', 'pool_name'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'prices']
) }}

-- block level prices by pool
-- step 1 what is the USD pool with the highest balance (aka deepest pool)
with blocks as (
    select
        height as block_id,
        b.block_timestamp,
        pool_name,
        rune_e8,
        asset_e8
    from
        {{ ref('thorchain_silver_block_pool_depths') }} bpd
    join {{ ref('thorchain_silver_block_log') }} bl
        on bpd.block_timestamp = bl.timestamp
    {% if is_incremental() -%}
    WHERE {{ incremental_predicate('CAST(from_unixtime(CAST(bpd.block_timestamp / 1e9 AS bigint)) AS timestamp)') }}
    {% endif -%}
)
, price as (
    select
        height as block_id,
        b.block_timestamp,
        rune_price_e8 as rune_usd
    from
        {{ ref('thorchain_silver_rune_price') }} rp
    join {{ ref('thorchain_silver_block_log') }} bl
        on rp.block_timestamp = bl.timestamp
    {% if is_incremental() -%}
    WHERE {{ incremental_predicate('CAST(from_unixtime(CAST(rp.block_timestamp / 1e9 AS bigint)) AS timestamp)') }}
    {% endif -%}
)
-- step 3 calculate the prices of assets by pool, in terms of tokens per tokens
-- and in USD for both tokens
SELECT DISTINCT 
    b.block_id,
    b.block_timestamp,
    COALESCE(
        b.rune_e8 / b.asset_e8,
        0
    ) AS price_rune_asset,
    COALESCE(
        b.asset_e8 / b.rune_e8,
        0
    ) AS price_asset_rune,
    COALESCE(ru.rune_usd * (b.rune_e8 / b.asset_e8), 0) AS asset_usd,
    COALESCE(
        ru.rune_usd,
        0
    ) AS rune_usd,
    b.pool_name,
    concat_ws(
        '-',
        b.block_id :: STRING,
        b.pool_name :: STRING
    ) AS _unique_key
FROM
    blocks as b
JOIN price as ru
    ON b.block_id = ru.block_id
WHERE
    b.rune_e8 > 0
    AND b.asset_e8 > 0