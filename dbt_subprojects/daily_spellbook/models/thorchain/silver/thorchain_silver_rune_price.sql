{{ config(
    schema = 'thorchain_silver',
    alias = 'rune_price',
    tags = ['thorchain', 'rune', 'prices']
) }}

with base as (
    SELECT
        rune_price_e8,
        block_timestamp,
        _ingested_at as _inserted_timestamp,
        row_number() over(partition by block_timestamp order by rune_price_e8 desc) as rn
    FROM {{ source('thorchain', 'rune_price') }}
)
SELECT *
FROM base
WHERE rn = 1