{{ config(
    schema = 'thorchain_silver',
    alias = 'upgrades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['_unique_key'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'upgrades', 'silver']
) }}

-- Switch events enriched with pricing for token upgrades
WITH block_prices AS (
    SELECT
        AVG(price) AS rune_usd,
        block_id
    FROM {{ ref('thorchain_silver_prices') }}
    WHERE symbol = 'RUNE'
    GROUP BY block_id
),

switch_events AS (
    SELECT
        tx AS tx_id,
        from_addr AS from_address,
        to_addr AS to_address,
        burn_asset,
        burn_e8,
        mint_e8,
        event_id,
        block_timestamp,
        _updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY tx, from_addr, to_addr, burn_asset, burn_e8, mint_e8, block_timestamp
            ORDER BY _updated_at DESC
        ) AS rn
    FROM {{ source('thorchain', 'switch_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
)

SELECT
    bl.height AS block_id,
    cast(from_unixtime(cast(se.block_timestamp / 1e9 as bigint)) as timestamp) AS block_time,
    date(from_unixtime(cast(se.block_timestamp / 1e9 as bigint))) AS block_date,
    date_trunc('month', from_unixtime(cast(se.block_timestamp / 1e9 as bigint))) AS block_month,
    se.tx_id,
    se.from_address,
    se.to_address,
    se.burn_asset,
    se.burn_e8 / power(10, 8) AS rune_amount,
    (se.burn_e8 / power(10, 8)) * COALESCE(p.rune_usd, 0) AS rune_amount_usd,
    se.mint_e8 / power(10, 8) AS mint_amount,
    (se.mint_e8 / power(10, 8)) * COALESCE(p.rune_usd, 0) AS mint_amount_usd,
    concat_ws(
        '-',
        se.tx_id,
        cast(se.block_timestamp as varchar),
        se.from_address,
        se.to_address,
        se.burn_asset
    ) AS _unique_key,
    current_timestamp AS _inserted_timestamp
FROM switch_events se
JOIN {{ source('thorchain', 'block_log') }} bl
    ON se.block_timestamp = bl.timestamp
LEFT JOIN block_prices p
    ON bl.height = p.block_id
WHERE se.rn = 1
  AND cast(from_unixtime(cast(se.block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
{% if is_incremental() %}
  AND {{ incremental_predicate('cast(from_unixtime(cast(se.block_timestamp / 1e9 as bigint)) as timestamp)') }}
{% endif %}

