{{ config(
    schema='sui_tvl',
    alias='lending_pools_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['protocol', 'market_id', 'object_id', 'version'],
    partition_by=['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags=['sui','tvl','lending']
) }}

-- All lending pools for TVL calculation (Bronze Layer)
-- Covers: Suilend, Navi, Scallop, Bucket, Alphalend
-- This model unions all protocol-specific bronze models

select 
    timestamp_ms
    , block_time
    , block_date
    , block_month
    , date_hour
    , protocol
    , market_id
    , coin_type
    , coin_collateral_amount
    , coin_borrow_amount
    , object_id
    , version
    , checkpoint 
from {{ ref('sui_tvl_lending_pools_suilend_bronze') }}
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}

union all

select 
    timestamp_ms
    , block_time
    , block_date
    , block_month
    , date_hour
    , protocol
    , market_id
    , coin_type
    , coin_collateral_amount
    , coin_borrow_amount
    , object_id
    , version
    , checkpoint 
from {{ ref('sui_tvl_lending_pools_navi_bronze') }}
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}

union all

select 
    timestamp_ms
    , block_time
    , block_date
    , block_month
    , date_hour
    , protocol
    , market_id
    , coin_type
    , coin_collateral_amount
    , coin_borrow_amount
    , object_id
    , version
    , checkpoint 
from {{ ref('sui_tvl_lending_pools_scallop_bronze') }}
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}

union all

select 
    timestamp_ms
    , block_time
    , block_date
    , block_month
    , date_hour
    , protocol
    , market_id
    , coin_type
    , coin_collateral_amount
    , coin_borrow_amount
    , object_id
    , version
    , checkpoint 
from {{ ref('sui_tvl_lending_pools_bucket_bronze') }}
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}

union all

select 
    timestamp_ms
    , block_time
    , block_date
    , block_month
    , date_hour
    , protocol
    , market_id
    , coin_type
    , coin_collateral_amount
    , coin_borrow_amount
    , object_id
    , version
    , checkpoint 
from {{ ref('sui_tvl_lending_pools_alphalend_bronze') }}
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %} 