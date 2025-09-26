{{ config(
    schema='sui_tvl',
    alias='lending_pools_alphalend_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['protocol', 'market_id', 'block_date'],
    partition_by=['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags=['sui','tvl','lending','alphalend']
) }}

-- Alphalend lending pools for TVL calculation (Bronze Layer)

{% set sui_project_start_date = var('sui_project_start_date', '2025-09-25') %}

select
    timestamp_ms
    , from_unixtime(timestamp_ms/1000) as block_time
    , date(from_unixtime(timestamp_ms/1000)) as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month
    , date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as date_hour
    , 'alphalend' as protocol
    , json_extract_scalar(object_json, '$.value.market_id') as market_id
    , concat('0x', json_extract_scalar(object_json, '$.value.coin_type.name')) as coin_type
    , (cast(json_extract_scalar(object_json, '$.value.balance_holding') as decimal(38,0)) + 
     cast(json_extract_scalar(object_json, '$.value.borrowed_amount') as decimal(38,0))) as coin_collateral_amount
    , cast(json_extract_scalar(object_json, '$.value.borrowed_amount') as decimal(38,0)) as coin_borrow_amount
    , object_id
    , version
    , checkpoint
from {{ source('sui','objects') }}
where cast(type_ as varchar) = '0x2::dynamic_field::Field<u64, 0xd631cd66138909636fc3f73ed75820d0c5b76332d1644608ed1c85ea2b8219b4::market::Market>'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ sui_project_start_date }}'
{% if is_incremental() %}
and checkpoint > coalesce((select max(checkpoint) from {{ this }}), 0)
and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
{% endif %} 