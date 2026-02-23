{{ config(
    schema = 'thorchain_silver',
    alias = 'prices',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', '_unique_key'],
    partition_by = ['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'prices']
) }}

-- block level prices by pool
-- step 1 what is the USD pool with the highest balance (aka deepest pool)
with blocks as (
    select
        bl.height as block_id,
        bl.block_timestamp,
        bpd.pool_name,
        bpd.rune_e8,
        bpd.asset_e8
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
        bl.height as block_id,
        bl.block_timestamp,
        rp.rune_price_e8 as rune_usd
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
    cast(date_trunc('day', b.block_timestamp) as date) as block_date,
    b.block_timestamp,
    COALESCE(
        cast(b.rune_e8 as double) / nullif(cast(b.asset_e8 as double), 0),
        0
    ) AS price_rune_asset,
    COALESCE(
        cast(b.asset_e8 as double) / nullif(cast(b.rune_e8 as double), 0),
        0
    ) AS price_asset_rune,
    COALESCE(
        ru.rune_usd * (cast(b.rune_e8 as double) / nullif(cast(b.asset_e8 as double), 0)),
        0
    ) AS asset_usd,
    COALESCE(
        ru.rune_usd,
        0
    ) AS rune_usd,
    b.pool_name,
    concat_ws(
        '-',
        cast(b.block_id as varchar),
        cast(b.pool_name as varchar)
    ) AS _unique_key
FROM
    blocks as b
JOIN price as ru
    ON b.block_id = ru.block_id
WHERE
    b.rune_e8 > 0
    AND b.asset_e8 > 0