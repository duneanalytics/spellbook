{{ config(
    schema = 'thorchain_silver',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['_unique_key'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'transfers', 'silver']
) }}

WITH block_prices AS (
    SELECT
        AVG(rune_usd) AS rune_usd,
        block_id
    FROM {{ ref('thorchain_silver_prices') }}
    GROUP BY block_id
),

base AS (
    SELECT
        se.from_address,
        se.to_address,
        se.asset,
        se.amount_e8,
        se.event_id,
        se.block_timestamp,
        se.block_time,
        se.block_date,
        se.block_month,
        se._inserted_timestamp,
        b.height AS block_id,
        p.rune_usd
    FROM {{ ref('thorchain_silver_transfer_events') }} se
    JOIN {{ source('thorchain', 'block_log') }} b
        ON se.block_timestamp = b.timestamp
    LEFT JOIN block_prices p
        ON b.height = p.block_id
    WHERE se.block_time >= current_date - interval '14' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('se.block_time') }}
    {% endif %}
)

SELECT
    block_time,
    block_timestamp,
    block_date,
    block_month,
    block_id,
    from_address,
    to_address,
    asset,
    COALESCE(amount_e8 / POWER(10, 8), 0) AS rune_amount,
    COALESCE((amount_e8 / POWER(10, 8)) * rune_usd, 0) AS rune_amount_usd,
    event_id,
    concat_ws(
        '-',
        cast(block_id as varchar),
        cast(from_address as varchar),
        cast(to_address as varchar),
        cast(asset as varchar),
        cast(event_id as varchar)
    ) AS _unique_key,
    _inserted_timestamp
FROM base

