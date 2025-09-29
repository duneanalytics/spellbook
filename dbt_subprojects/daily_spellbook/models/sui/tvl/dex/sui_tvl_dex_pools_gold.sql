{{ config(
    schema='sui_tvl',
    alias='dex_pools_gold',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['block_date', 'protocol', 'pool_id'],
    partition_by=['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags=['sui','tvl','dex','gold']
) }}

-- Gold layer: BTC DEX pools with pricing
-- Filters for BTC pools using dynamic BTC tokens list
with btc_tokens as (
    -- Dynamically get all BTC-related tokens from btc_tokens_detail
    select distinct coin_type, coin_symbol
    from {{ ref('sui_tvl_btc_tokens_detail') }}
)

, btc_pools_silver as (
    -- Filter for pools containing ANY BTC token
    select s.*
    from {{ ref('sui_tvl_dex_pools_silver') }} s
    where exists (
        select 1 from btc_tokens bt 
        where s.coin_type_a = bt.coin_type 
           or s.coin_type_b = bt.coin_type
    )
    {% if is_incremental() %}
    and {{ incremental_predicate('block_date') }}
    {% endif %}
)

, daily_volume as (
    -- Aggregate daily volume per pool from trades (with overflow protection)
    select 
        block_date
        , pool_id
        , sum(case 
            when amount_usd > 1e12 or amount_usd < 0 then 0  -- Filter out suspicious values (>$1T per trade)
            else coalesce(amount_usd, 0) 
        end) as total_volume_usd
    from {{ ref('dex_sui_trades') }}
    where pool_id is not null
    {% if is_incremental() %}
    and {{ incremental_predicate('block_date') }}
    {% endif %}
    group by block_date, pool_id
)

, dex_pools_with_pricing as (
    select
        s.block_date
        , s.protocol
        , s.pool_name
        , s.pool_id
        , s.coin_a_symbol
        , s.coin_b_symbol
        , s.coin_type_a
        , s.coin_type_b
        , s.avg_coin_a_amount
        , s.avg_coin_b_amount
        , s.avg_fee_rate_percent
        , s.num_records
        
        -- Add volume data
        , coalesce(v.total_volume_usd, 0) as total_volume_usd
        
        -- Get pricing for coin A (with BTC proxy fallback for BTC tokens only)
        , coalesce(pa.price, 
            case when bta.coin_type is not null  -- Only use TBTC fallback if this coin is actually BTC
                 then tbtc_price.price else null end
        ) as coin_a_price_usd
        -- Get pricing for coin B (with BTC proxy fallback for BTC tokens only)
        , coalesce(pb.price, 
            case when btb.coin_type is not null  -- Only use TBTC fallback if this coin is actually BTC
                 then tbtc_price.price else null end
        ) as coin_b_price_usd
        
        -- Calculate TVL in USD (following existing decimal precision pattern)
        , cast(coalesce(cast(s.avg_coin_a_amount as double) * 
                coalesce(pa.price, 
                    case when bta.coin_type is not null 
                         then tbtc_price.price else null end), 0) + 
             coalesce(cast(s.avg_coin_b_amount as double) * 
                coalesce(pb.price, 
                    case when btb.coin_type is not null 
                         then tbtc_price.price else null end), 0) as decimal(38,8)) as tvl_usd
        
    from btc_pools_silver s
    
    -- Join volume data
    left join daily_volume v
        on v.block_date = s.block_date
        and v.pool_id = s.pool_id
    
    -- Check if coin A is a BTC token
    left join btc_tokens bta on s.coin_type_a = bta.coin_type
    
    -- Check if coin B is a BTC token  
    left join btc_tokens btb on s.coin_type_b = btb.coin_type
    
    -- Join pricing for coin A (use latest price for the day)
    left join (
        select 
            blockchain
            , contract_address
            , date(minute) as price_date
            , price
            , row_number() over (partition by blockchain, contract_address, date(minute) order by minute desc) as rn
        from {{ source('prices','usd') }}
        where blockchain = 'sui'
    ) pa on pa.blockchain = 'sui'
        and pa.price_date = s.block_date
        and pa.rn = 1
        and pa.contract_address = cast(
            regexp_replace(
                split_part(
                    case
                        when starts_with(lower(s.coin_type_a),'0x') then lower(s.coin_type_a)
                        else concat('0x', lower(s.coin_type_a))
                    end, '::', 1
                )
                , '^0x0*([0-9a-f]+)$', '0x$1'
            ) as varbinary
        )
    
    -- Join pricing for coin B (use latest price for the day)
    left join (
        select 
            blockchain
            , contract_address
            , date(minute) as price_date
            , price
            , row_number() over (partition by blockchain, contract_address, date(minute) order by minute desc) as rn
        from {{ source('prices','usd') }}
        where blockchain = 'sui'
    ) pb on pb.blockchain = 'sui'
        and pb.price_date = s.block_date
        and pb.rn = 1
        and pb.contract_address = cast(
            regexp_replace(
                split_part(
                    case
                        when starts_with(lower(s.coin_type_b),'0x') then lower(s.coin_type_b)
                        else concat('0x', lower(s.coin_type_b))
                    end, '::', 1
                )
                , '^0x0*([0-9a-f]+)$', '0x$1'
            ) as varbinary
        )
    
    -- Join TBTC pricing as BTC proxy
    left join (
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
    ) tbtc_price on tbtc_price.blockchain = 'sui'
        and tbtc_price.price_date = s.block_date
        and tbtc_price.rn = 1
)

select
    block_date
    , protocol
    , pool_name
    , pool_id
    , coin_a_symbol
    , coin_b_symbol
    , coin_type_a
    , coin_type_b
    , avg_coin_a_amount
    , avg_coin_b_amount
    , coin_a_price_usd
    , coin_b_price_usd
    , tvl_usd
    , total_volume_usd
    , avg_fee_rate_percent as fee_rate_percent
    , num_records
from dex_pools_with_pricing
where tvl_usd > 1000 and total_volume_usd > 1000
order by
    block_date desc
    , tvl_usd desc 