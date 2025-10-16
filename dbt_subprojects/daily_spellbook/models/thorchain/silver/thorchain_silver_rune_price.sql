{{ config(
    schema = 'thorchain_silver',
    alias = 'rune_price',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'block_time'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'rune', 'prices']
) }}

with base as (
    SELECT
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) as block_time,
        date(from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_date,
        date_trunc('month', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_month,
        block_timestamp as raw_block_timestamp,
        rune_price_e8 as rune_price_usd,
        rune_price_e8,
        'RUNE' as symbol,
        'thorchain' as blockchain,
        cast(null as varbinary) as contract_address -- RUNE is native token
    FROM {{ source('thorchain', 'rune_price') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '17' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
)

SELECT * FROM base
