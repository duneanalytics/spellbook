{{ config(
    schema = 'thorchain_silver',
    alias = 'rune_price',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_timestamp'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'rune', 'prices']
) }}

SELECT
    cast(from_unixtime(block_timestamp / 1e18) as timestamp) as block_time,
    date(from_unixtime(block_timestamp / 1e18)) as block_date,
    date_trunc('month', from_unixtime(block_timestamp / 1e18)) as block_month,
    block_timestamp as raw_block_timestamp,
    rune_price_e8 / 1e8 as rune_price_usd,
    rune_price_e8,
    'RUNE' as symbol,
    'thorchain' as blockchain,
    cast(null as varbinary) as contract_address -- RUNE is native token
FROM {{ source('thorchain', 'rune_price') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('cast(from_unixtime(block_timestamp / 1e18) as timestamp)') }}
{% endif %}
