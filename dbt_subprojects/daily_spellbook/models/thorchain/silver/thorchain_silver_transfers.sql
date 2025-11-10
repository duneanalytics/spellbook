{{ config(
    schema = 'thorchain_silver',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', '_unique_key'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'transfers', 'silver']
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
SELECT
    cast(date_trunc('day', b.block_timestamp) AS date) AS day,
    b.block_timestamp,
    b.height AS block_id,
    cast(from_address as varchar) as from_address,
    cast(to_address as varchar) as to_address,
    cast(asset as varchar) as asset,
    COALESCE(amount_e8 / pow(10, 8), 0) AS rune_amount,
    COALESCE(amount_e8 / pow(10, 8) * p.rune_usd, 0) AS rune_amount_usd,
    cast(event_id as varchar) as event_id,
    concat_ws(
        cast(b.height as varchar),
        cast(from_address as varchar),
        cast(to_address as varchar),
        cast(asset as varchar),
        cast(event_id as varchar)
    ) AS _unique_key,
    se._inserted_timestamp
FROM
    {{ ref('thorchain_silver_transfer_events') }} as se
JOIN {{ ref('thorchain_silver_block_log') }} as b
    ON se.block_timestamp = b.timestamp
LEFT JOIN block_prices as p
    ON b.height = p.block_id
{% if is_incremental() or true %}
WHERE
    {{ incremental_predicate('b.block_timestamp') }}
{% endif %}