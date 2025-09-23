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

-- Gold layer: DEX pools with pricing and business metrics
-- Adds USD pricing and applies business filters like the Snowflake pattern

with combined_coin_info as (
    -- Use existing coin info model and add SUI if not present
    select
        coin_type,
        coin_decimals,
        coin_symbol
    from {{ ref('dex_sui_coin_info') }}
    
    union all
    
    select
        '0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI' as coin_type,
        9 as coin_decimals,
        'SUI' as coin_symbol
),

dex_pools_with_pricing as (
    select
        s.block_date,
        s.protocol,
        s.pool_name,
        s.pool_id,
        s.coin_a_symbol,
        s.coin_b_symbol,
        s.coin_type_a,
        s.coin_type_b,
        s.avg_coin_a_amount,
        s.avg_coin_b_amount,
        s.avg_fee_rate_percent,
        s.num_records,
        
        -- Get pricing for coin A
        pa.price as coin_a_price_usd,
        -- Get pricing for coin B  
        pb.price as coin_b_price_usd,
        
        -- Calculate TVL in USD (following existing decimal precision pattern)
        cast(coalesce(cast(s.avg_coin_a_amount as double) * pa.price, 0) + 
             coalesce(cast(s.avg_coin_b_amount as double) * pb.price, 0) as decimal(38,8)) as tvl_usd
        
    from {{ ref('sui_tvl_dex_pools_silver') }} s
    
    -- Join pricing for coin A
    left join {{ source('prices','usd') }} pa
        on pa.blockchain = 'sui'
        and date(pa.minute) = s.block_date
        and pa.contract_address = cast(
            regexp_replace(
                split_part(
                    case
                        when starts_with(lower(s.coin_type_a),'0x') then lower(s.coin_type_a)
                        else concat('0x', lower(s.coin_type_a))
                    end, '::', 1
                ),
                '^0x0*([0-9a-f]+)$', '0x$1'
            ) as varbinary
        )
    
    -- Join pricing for coin B
    left join {{ source('prices','usd') }} pb
        on pb.blockchain = 'sui'
        and date(pb.minute) = s.block_date
        and pb.contract_address = cast(
            regexp_replace(
                split_part(
                    case
                        when starts_with(lower(s.coin_type_b),'0x') then lower(s.coin_type_b)
                        else concat('0x', lower(s.coin_type_b))
                    end, '::', 1
                ),
                '^0x0*([0-9a-f]+)$', '0x$1'
            ) as varbinary
        )
    
    {% if is_incremental() %}
    where s.block_date >= date_sub(current_date(), 7)
    {% endif %}
)

select
    block_date as metric_date,
    protocol,
    pool_name,
    pool_id,
    coin_a_symbol,
    coin_b_symbol,
    coin_type_a,
    coin_type_b,
    avg_coin_a_amount,
    avg_coin_b_amount,
    coin_a_price_usd,
    coin_b_price_usd,
    tvl_usd,
    avg_fee_rate_percent as fee_rate_percent,
    num_records
from dex_pools_with_pricing
where tvl_usd > 100  -- Business filter: only include pools with meaningful TVL
order by
    metric_date desc,
    tvl_usd desc 