{{ config(
    schema='sui_tvl',
    alias='supply_silver',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags=['sui','tvl','supply','silver']
) }}

-- Silver layer: BTC supply with simple daily aggregation
-- Using dynamic BTC token discovery instead of static whitelist

with btc_tokens as (
    -- Get BTC tokens directly from bronze with address-based matching and manual symbol mapping
    select distinct
        coin_type,
        case 
            when coin_type like '%27792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881%' then 'BTC'
            when coin_type like '%77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1%' then 'TBTC'  
            when coin_type like '%dfe175720cb087f967694c1ce7e881ed835be73e8821e161c351f4cea24a0f20%' then 'SATLBTC'
        end as coin_symbol,
        case 
            when coin_type like '%27792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881%' then 8
            when coin_type like '%77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1%' then 8
            when coin_type like '%dfe175720cb087f967694c1ce7e881ed835be73e8821e161c351f4cea24a0f20%' then 8
        end as coin_decimals
    from {{ ref('sui_tvl_supply_bronze') }}
    where coin_type like '%27792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881%'
       or coin_type like '%77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1%'
       or coin_type like '%dfe175720cb087f967694c1ce7e881ed835be73e8821e161c351f4cea24a0f20%'
),

daily_btc_supply as (
    -- Get latest supply per BTC token per day
    select
        s.block_date,
        s.coin_type,
        s.total_supply,
        bt.coin_symbol,
        bt.coin_decimals,
        row_number() over (
            partition by s.block_date, s.coin_type
            order by s.timestamp_ms desc
        ) as rn
    from {{ ref('sui_tvl_supply_bronze') }} s
    inner join btc_tokens bt on s.coin_type = bt.coin_type
    {% if is_incremental() %}
    where {{ incremental_predicate('s.block_date') }}
    {% endif %}
),

deduplicated_daily_btc_supply_by_symbol as (
    -- Handle multiple tokens with same symbol (e.g., different wrapped BTC versions)
    select
        block_date,
        coin_symbol,
        total_supply,
        coin_decimals
    from (
        select
            block_date,
            coin_symbol,
            total_supply,
            coin_decimals,
            row_number() over (
                partition by block_date, coin_symbol
                order by total_supply desc, coin_type desc -- Largest supply wins, tie-break by coin_type
            ) as rn
        from daily_btc_supply
        where rn = 1  -- Filter for latest supply per token per day first
    )
    where rn = 1  -- Then deduplicate by symbol
)

select
    block_date,
    
    -- Total BTC supply across all variants
    sum(total_supply / power(10, coin_decimals)) as total_btc_supply,
    
    -- Supply breakdown by symbol (JSON aggregation using Trino map_agg)
    map_agg(
        coin_symbol, 
        round(total_supply / power(10, coin_decimals), 0)
    ) as supply_breakdown_json
    
from deduplicated_daily_btc_supply_by_symbol
group by block_date
order by block_date desc 