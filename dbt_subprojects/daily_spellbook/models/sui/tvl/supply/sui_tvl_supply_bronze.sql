{{ config(
    schema='sui_tvl',
    alias='supply_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['coin_type', 'block_date'],
    tags=['sui','tvl','supply']
) }}

-- Tracks the minted on-chain supply of tokens (Bronze Layer)
-- This table captures the minted on-chain supply of tokens
-- Converted from Snowflake materialized view to dbt incremental model

with supply_data as (
    select
        date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as hour_timestamp,
        from_unixtime(timestamp_ms/1000) as block_time,
        date(from_unixtime(timestamp_ms/1000)) as block_date,
        date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month,
        timestamp_ms,
        cast(type_ as varchar) as type_,
        object_status,
        version,
        object_id,
        checkpoint,
        -- Extract coin type based on the specific pattern found in TYPE
        case
            -- For non-generic TreasuryCapManager (e.g., ...::mbtc::TreasuryCapManager)
            when cast(type_ as varchar) like '%::TreasuryCapManager' 
            then replace(cast(type_ as varchar), '::TreasuryCapManager', '::' || upper(split_part(cast(type_ as varchar), '::', 2)))
            
            -- For TreasuryCap, extract the type within the outermost <...>
            when cast(type_ as varchar) like '0x2::coin::TreasuryCap<%>%' 
            then regexp_extract(cast(type_ as varchar), '<([^>]+)>', 1)
            
            -- For ControlledTreasury, extract the type within the outermost <...>
            when cast(type_ as varchar) like '%::treasury::ControlledTreasury<%>%' 
            then regexp_extract(cast(type_ as varchar), '<([^>]+)>', 1)
            
            -- For Dynamic Fields containing Token Registry Keys and Wrapped Assets
            when cast(type_ as varchar) like '0x2::dynamic_field::Field<%::token_registry::Key<%>%wrapped_asset::WrappedAsset<%>%' 
            then regexp_extract(cast(type_ as varchar), '::token_registry::Key<([^>]+)>', 1)
            
            -- For TBTC GatewayCapabilities, use specific coin type as provided
            when cast(type_ as varchar) = '0x77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1::Gateway::GatewayCapabilities' 
            then '0x77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1::TBTC::TBTC'
            
            -- For SATLBTC Vault, use specific coin type as provided
            when cast(type_ as varchar) like '0x25646e1cac13d6198e821aac7a94cbb74a8e49a2b3bed2ffd22346990811fcc6::satlayer_pool::Vault<%>%' 
            then '0xdfe175720cb087f967694c1ce7e881ed835be73e8821e161c351f4cea24a0f20::satlbtc::SATLBTC'
            
            else null
        end as coin_type,
        
        -- Extract total supply, checking potential JSON paths based on TYPE patterns
        case
            -- For non-generic TreasuryCapManager (path: treasury:total_supply:value)
            when cast(type_ as varchar) like '%::TreasuryCapManager' 
            then cast(json_extract_scalar(object_json, '$.treasury.total_supply.value') as double)
            
            -- For TreasuryCap (top-level total_supply:value)
            when cast(type_ as varchar) like '0x2::coin::TreasuryCap<%>%' 
            then cast(json_extract_scalar(object_json, '$.total_supply.value') as double)
            
            -- For ControlledTreasury, TBTC GatewayCapabilities, SATLBTC Vault (treasury_cap:total_supply:value)
            when cast(type_ as varchar) like '%::treasury::ControlledTreasury<%>%'
                 or cast(type_ as varchar) = '0x77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1::Gateway::GatewayCapabilities'
                 or cast(type_ as varchar) like '0x25646e1cac13d6198e821aac7a94cbb74a8e49a2b3bed2ffd22346990811fcc6::satlayer_pool::Vault<%>%'
            then cast(json_extract_scalar(object_json, '$.treasury_cap.total_supply.value') as double)
            
            -- For Dynamic Fields (Token Registry/Wrapped Asset) (value:treasury_cap:total_supply:value)
            when cast(type_ as varchar) like '0x2::dynamic_field::Field<%::token_registry::Key<%>%wrapped_asset::WrappedAsset<%>%' 
            then cast(json_extract_scalar(object_json, '$.value.treasury_cap.total_supply.value') as double)
            
            else null
        end as total_supply
        
    from {{ source('sui','objects') }}
    where 
        -- Pre-filter for rows matching any of the relevant type structures
        (
            cast(type_ as varchar) like '0x2::coin::TreasuryCap<%>%' or
            cast(type_ as varchar) like '%::treasury::ControlledTreasury<%>%' or
            cast(type_ as varchar) like '0x2::dynamic_field::Field<%::token_registry::Key<%>%wrapped_asset::WrappedAsset<%>%' or
            cast(type_ as varchar) = '0x77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1::Gateway::GatewayCapabilities' or
            cast(type_ as varchar) like '0x25646e1cac13d6198e821aac7a94cbb74a8e49a2b3bed2ffd22346990811fcc6::satlayer_pool::Vault<%>%' or
            cast(type_ as varchar) like '%::TreasuryCapManager'
        )
        {% if is_incremental() %}
        and checkpoint > coalesce((select max(checkpoint) from {{ this }}), 0)
        {% endif %}
)

select 
    hour_timestamp,
    block_time,
    block_date,
    block_month,
    timestamp_ms,
    type_,
    coin_type,
    total_supply,
    version,
    object_id,
    object_status,
    checkpoint
from supply_data
where 
    -- Post-filter to ensure coin_type was extracted and total_supply is valid
    coin_type is not null
    and total_supply is not null 