{{ config(
    schema='sui_tvl',
    alias='supply_gold',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['block_date'],
    partition_by=['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags=['sui','tvl','supply','gold']
) }}

-- Gold layer: Token supply with USD valuation and final metrics
-- Following the Snowflake pattern with pricing integration

with supply_with_pricing as (
    select
        s.block_date,
        s.total_token_supply,
        s.supply_breakdown_json,
        s.token_count,
        
        -- Get SUI price for valuation (using SUI as the primary token)
        p.price as sui_price_usd,
        
        -- Calculate rough USD valuation (simplified approach)
        cast(coalesce(cast(s.total_token_supply as double) * p.price, 0) as decimal(38,8)) as estimated_total_supply_usd
        
    from {{ ref('sui_tvl_supply_silver') }} s
    
    -- Join SUI pricing as a proxy for ecosystem valuation
    left join {{ source('prices','usd') }} p
        on p.blockchain = 'sui'
        and date(p.minute) = s.block_date
        and p.contract_address = cast('0x0000000000000000000000000000000000000000000000000000000000000002' as varbinary)
    
    {% if is_incremental() %}
    where s.block_date >= date_sub(current_date(), 7)
    {% endif %}
)

select
    block_date,
    total_token_supply,
    supply_breakdown_json,
    token_count,
    sui_price_usd,
    estimated_total_supply_usd,
    
    -- Calculate metrics
    case 
        when lag(total_token_supply) over (order by block_date) is not null 
        then ((total_token_supply - lag(total_token_supply) over (order by block_date)) / lag(total_token_supply) over (order by block_date)) * 100
        else null 
    end as daily_supply_change_percent,
    
    case 
        when lag(estimated_total_supply_usd) over (order by block_date) is not null 
        then ((estimated_total_supply_usd - lag(estimated_total_supply_usd) over (order by block_date)) / lag(estimated_total_supply_usd) over (order by block_date)) * 100
        else null 
    end as daily_usd_value_change_percent
    
from supply_with_pricing
where total_token_supply > 0 -- Business filter: only meaningful supply
order by block_date desc 