{{ config(
    schema='sui_tvl',
    alias='lending_pools_gold',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['block_date', 'protocol', 'collateral_coin_symbol'],
    partition_by=['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags=['sui','tvl','lending','gold']
) }}

-- Gold layer: Lending pools with USD pricing and token-level aggregation
-- Following the Snowflake pattern exactly: aggregate by token symbol per protocol

with lending_with_pricing as (
    select
        s.block_date,
        s.protocol,
        s.market_id,
        s.collateral_coin_type,
        s.collateral_coin_symbol,
        s.borrow_coin_type,
        s.borrow_coin_symbol,
        s.eod_collateral_amount,
        s.eod_borrow_amount,
        s.latest_block_time,
        
        -- Get pricing for collateral token
        pc.price as collateral_price_usd,
        -- Get pricing for borrow token  
        pb.price as borrow_price_usd,
        
        -- Calculate USD values (following existing decimal precision pattern)
        cast(coalesce(cast(s.eod_collateral_amount as double) * pc.price, 0) as decimal(38,8)) as collateral_usd,
        cast(coalesce(cast(s.eod_borrow_amount as double) * pb.price, 0) as decimal(38,8)) as borrow_usd,
        cast(coalesce(cast(s.eod_collateral_amount as double) * pc.price, 0) - 
             coalesce(cast(s.eod_borrow_amount as double) * pb.price, 0) as decimal(38,8)) as net_supply_usd
        
    from {{ ref('sui_tvl_lending_pools_silver') }} s
    
    -- Join pricing for collateral token
    left join {{ source('prices','usd') }} pc
        on pc.blockchain = 'sui'
        and date(pc.minute) = s.block_date
        and pc.contract_address = cast(
            regexp_replace(
                split_part(
                    case
                        when starts_with(lower(s.collateral_coin_type),'0x') then lower(s.collateral_coin_type)
                        else concat('0x', lower(s.collateral_coin_type))
                    end, '::', 1
                ),
                '^0x0*([0-9a-f]+)$', '0x$1'
            ) as varbinary
        )
    
    -- Join pricing for borrow token
    left join {{ source('prices','usd') }} pb
        on pb.blockchain = 'sui'
        and date(pb.minute) = s.block_date
        and pb.contract_address = cast(
            regexp_replace(
                split_part(
                    case
                        when starts_with(lower(s.borrow_coin_type),'0x') then lower(s.borrow_coin_type)
                        else concat('0x', lower(s.borrow_coin_type))
                    end, '::', 1
                ),
                '^0x0*([0-9a-f]+)$', '0x$1'
            ) as varbinary
        )
    
    {% if is_incremental() %}
    where s.block_date >= date_sub(current_date(), 7)
    {% endif %}
)

-- Following Snowflake pattern: aggregate by token symbol per protocol per day
select
    block_date,
    protocol,
    collateral_coin_symbol,
    collateral_coin_type,
    
    -- Aggregate amounts across all markets for this token (following Snowflake pattern)
    sum(eod_collateral_amount) as total_collateral_amount,
    sum(eod_borrow_amount) as total_borrow_amount,
    sum(eod_collateral_amount - eod_borrow_amount) as net_supply_amount,
    
    -- Aggregate USD values (following Snowflake pattern)
    sum(collateral_usd) as total_collateral_usd,
    sum(borrow_usd) as total_borrow_usd,
    sum(net_supply_usd) as net_tvl_usd,
    
    -- TVL is total collateral locked (following Snowflake pattern)
    sum(collateral_usd) as tvl_usd,
    
    -- Metadata
    avg(collateral_price_usd) as avg_collateral_price_usd,
    count(distinct market_id) as market_count,
    max(latest_block_time) as latest_block_time
    
from lending_with_pricing
where collateral_usd > 1000
group by 
    block_date,
    protocol,
    collateral_coin_symbol,
    collateral_coin_type
order by
    block_date desc,
    tvl_usd desc 