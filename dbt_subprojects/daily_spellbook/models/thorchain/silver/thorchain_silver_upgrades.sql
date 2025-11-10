{{ config(
    schema = 'thorchain_silver',
    alias = 'upgrades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', '_unique_key'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'upgrades', 'silver']
) }}

WITH block_prices AS (
    SELECT
        AVG(rune_usd) AS rune_usd,
        block_id
    FROM
        {{ ref('thorchain_silver_prices') }}
    GROUP BY
        block_id
)
, switch_events AS (
    SELECT
        tx AS tx_id,
        from_addr AS from_address,
        to_addr AS to_address,
        burn_asset,
        burn_e8,
        mint_e8,
        event_id,
        block_timestamp,
        _inserted_timestamp,
        row_number() over(
            PARTITION BY tx, from_addr, to_addr, burn_asset, burn_e8, mint_e8, block_timestamp
            ORDER BY _inserted_timestamp DESC
        ) as rn
    FROM
    {{ source('thorchain', 'switch_events') }}
)
SELECT
    cast(date_trunc('day', b.block_timestamp) AS date) AS day,
    b.block_timestamp,
    b.height AS block_id,
    tx_id,
    from_address,
    to_address,
    burn_asset,
    burn_e8 / pow(
        10,
        8
    ) AS rune_amount,
    burn_e8 / pow(
        10,
        8
    ) * p.rune_usd AS rune_amount_usd,
    mint_e8 / pow(
        10,
        8
    ) AS mint_amount,
    mint_e8 / pow(
        10,
        8
    ) * p.rune_usd AS mint_amount_usd,
    concat_ws(
        '-',
        tx_id,
        cast(se.block_timestamp as varchar),
        cast(from_address as varchar),
        cast(to_address as varchar),
        cast(burn_asset as varchar)
    ) AS _unique_key,
    se._inserted_timestamp
FROM
  switch_events as se
JOIN {{ ref('thorchain_silver_block_log') }} as b
    ON se.block_timestamp = b.timestamp
LEFT JOIN block_prices as p
    ON b.height = p.block_id
WHERE
    se.rn = 1
    {% if is_incremental() -%}
    and {{ incremental_predicate('b.block_timestamp') }}
    {% endif -%}