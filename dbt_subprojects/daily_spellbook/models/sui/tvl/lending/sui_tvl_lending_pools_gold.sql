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

-- Gold layer: BTC lending pools with USD pricing and business metrics
-- Filters for BTC tokens only and adds USD valuation

with btc_lending_data as (
    select *
    from {{ ref('sui_tvl_lending_pools_silver') }}
    where collateral_coin_symbol like '%BTC%'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_date') }}
    {% endif %}
)

-- Get BTC price using TBTC as proxy (matches ecosystem pattern)
, btc_price_data as (
    select 
        blockchain
        , date(minute) as price_date
        , price
        , row_number() over (partition by blockchain, date(minute) order by minute desc) as rn
    from {{ source('prices','usd') }}
    where blockchain = 'sui'
    and contract_address = cast(
        regexp_replace(
            split_part(
                lower('0x77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1::TBTC::TBTC')
                , '::', 1
            )
            , '^0x0*([0-9a-f]+)$', '0x$1'
        ) as varbinary
    )
)

select
    l.block_date
    , l.protocol
    , l.collateral_coin_symbol
    
    -- BTC Native Amounts
    , sum(l.eod_collateral_amount) as btc_collateral
    , sum(
        case 
            when l.protocol = 'bucket' then 0  -- Bucket borrows BUCK, not BTC
            else l.eod_borrow_amount 
        end
    ) as btc_borrow
    , sum(
        case 
            when l.protocol = 'bucket' then l.eod_collateral_amount  -- For Bucket, supply = collateral (no BTC borrow)
            else (l.eod_collateral_amount - l.eod_borrow_amount)
        end
    ) as btc_supply
    
    -- USD Values
    , p.price as btc_price_usd
    , cast(coalesce(cast(sum(l.eod_collateral_amount) as double) * p.price, 0) as decimal(38,8)) as btc_collateral_usd
    , cast(coalesce(cast(sum(
        case 
            when l.protocol = 'bucket' then 0
            else l.eod_borrow_amount 
        end
    ) as double) * p.price, 0) as decimal(38,8)) as btc_borrow_usd
    , cast(coalesce(cast(sum(
        case 
            when l.protocol = 'bucket' then l.eod_collateral_amount
            else (l.eod_collateral_amount - l.eod_borrow_amount)
        end
    ) as double) * p.price, 0) as decimal(38,8)) as btc_supply_usd
    
from btc_lending_data l

-- Join BTC pricing (latest price per day)
left join btc_price_data p 
    on p.blockchain = 'sui'
    and p.price_date = l.block_date
    and p.rn = 1

group by
    l.block_date
    , l.protocol
    , l.collateral_coin_symbol
    , p.price
order by
    block_date desc
    , protocol
    , collateral_coin_symbol