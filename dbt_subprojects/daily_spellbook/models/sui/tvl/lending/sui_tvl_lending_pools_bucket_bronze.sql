{{ config(
    schema='sui_tvl',
    alias='lending_pools_bucket_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['protocol', 'market_id', 'block_date'],
    partition_by=['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags=['sui','tvl','lending','bucket']
) }}

-- Bucket lending pools for TVL calculation (Bronze Layer)

{% set sui_project_start_date = var('sui_project_start_date', '2025-09-25') %}

select
    timestamp_ms
    , from_unixtime(timestamp_ms/1000) as block_time
    , date(from_unixtime(timestamp_ms/1000)) as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month
    , date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as date_hour
    , 'bucket' as protocol
    , cast(object_id as varchar) as market_id
    , case 
        when starts_with(regexp_extract(cast(type_ as varchar), '<([^>]+)>', 1), '0x') 
        then regexp_extract(cast(type_ as varchar), '<([^>]+)>', 1)
        else concat('0x', regexp_extract(cast(type_ as varchar), '<([^>]+)>', 1))
    end as coin_type
    , cast(json_extract_scalar(object_json, '$.collateral_vault') as decimal(38,0)) as coin_collateral_amount
    , cast(json_extract_scalar(object_json, '$.minted_buck_amount') as decimal(38,0)) as coin_borrow_amount
    , object_id
    , version
    , checkpoint
from {{ source('sui','objects') }}
where cast(type_ as varchar) like '0xce7ff77a83ea0cb6fd39bd8748e2ec89a3f41e8efdc3f4eb123e0ca37b184db2::bucket::Bucket<%>'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ sui_project_start_date }}'
{% if is_incremental() %}
and checkpoint > coalesce((select max(checkpoint) from {{ this }}), 0)
and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
{% endif %} 