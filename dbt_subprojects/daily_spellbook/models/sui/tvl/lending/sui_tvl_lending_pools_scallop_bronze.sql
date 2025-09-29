{{ config(
    schema='sui_tvl',
    alias='lending_pools_scallop_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['protocol', 'market_id', 'object_id', 'version'],
    partition_by=['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags=['sui','tvl','lending','scallop']
) }}

-- Scallop lending pools for TVL calculation (Bronze Layer)

{% set sui_project_start_date = var('sui_project_start_date', '2023-06-01') %}

select
    timestamp_ms
    , from_unixtime(timestamp_ms/1000) as block_time
    , date(from_unixtime(timestamp_ms/1000)) as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month
    , date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as date_hour
    , 'scallop' as protocol
    , cast(object_id as varchar) as market_id
    , case 
        when starts_with(json_extract_scalar(object_json, '$.name.name'), '0x') 
        then json_extract_scalar(object_json, '$.name.name')
        else concat('0x', json_extract_scalar(object_json, '$.name.name'))
    end as coin_type
    , (cast(json_extract_scalar(object_json, '$.value.cash') as decimal(38,0)) + 
     cast(json_extract_scalar(object_json, '$.value.debt') as decimal(38,0))) as coin_collateral_amount
    , cast(json_extract_scalar(object_json, '$.value.debt') as decimal(38,0)) as coin_borrow_amount
    , object_id
    , version
    , checkpoint
from {{ source('sui','objects') }}
where cast(type_ as varchar) like '0x2::dynamic_field::Field<0x1::type_name::TypeName, 0xefe8b36d5b2e43728cc323298626b83177803521d195cfb11e15b910e892fddf::reserve::BalanceSheet>%'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ sui_project_start_date }}'
{% if is_incremental() %}
and checkpoint > coalesce((select max(checkpoint) from {{ this }}), 0)
and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
{% endif %} 