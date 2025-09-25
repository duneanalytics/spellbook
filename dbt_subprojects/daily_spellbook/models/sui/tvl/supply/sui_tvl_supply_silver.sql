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
-- Simplified to use latest snapshot per token per day (like other Sui models)

with btc_tokens as (
    -- Get BTC token whitelist
    select 
        coin_type,
        coin_symbol,
        coin_decimals
    from {{ ref('sui_tvl_btc_tokens_detail') }}
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
    block_date as date,
    
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