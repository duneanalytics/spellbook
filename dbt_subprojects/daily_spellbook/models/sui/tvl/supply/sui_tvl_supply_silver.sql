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

-- Silver layer: Daily aggregated token supply with LOCF (Last Observation Carried Forward)
-- Following the Snowflake pattern with dense date generation and supply tracking

with recursive_all_dates as (
    -- Generate all dates from the earliest supply record up to today
    select min(block_date) as dt
    from {{ ref('sui_tvl_supply_bronze') }}
    
    union all
    
    select date_add(dt, 1) as dt
    from recursive_all_dates
    where dt < current_date()
),

-- Get distinct tokens from coin info (not limiting to specific token types)
all_tokens as (
    select 
        coin_type,
        coin_symbol,
        coin_decimals
    from {{ ref('sui_tvl_tokens_detail') }}
    where coin_type is not null
),

-- Creating cross of dense dates and tokens
all_dates_and_coins as (
    select
        rd.dt as block_date,
        ci.coin_type,
        ci.coin_symbol,
        ci.coin_decimals
    from recursive_all_dates rd
    cross join all_tokens ci
),

-- Ensuring date/token density using LOCF
daily_supply_with_locf as (
    select
        adc.block_date,
        adc.coin_type,
        adc.coin_symbol,
        adc.coin_decimals,
        -- Apply Last Observation Carried Forward (LOCF) for total_supply
        last_value(su.total_supply) ignore nulls over (
            partition by adc.coin_type
            order by adc.block_date asc, su.timestamp_ms asc nulls last
            rows between unbounded preceding and current row
        ) as carried_forward_supply_raw,
        -- Also capture the timestamp of the last observed supply
        last_value(su.timestamp_ms) ignore nulls over (
            partition by adc.coin_type
            order by adc.block_date asc, su.timestamp_ms asc nulls last
            rows between unbounded preceding and current row
        ) as last_observed_timestamp_ms
    from all_dates_and_coins adc
    left join {{ ref('sui_tvl_supply_bronze') }} su
        on adc.coin_type = su.coin_type
        and su.timestamp_ms < unix_timestamp(date_add(adc.block_date, 1)) * 1000
    {% if is_incremental() %}
    where adc.block_date >= date_sub(current_date(), 7)
    {% endif %}
    qualify carried_forward_supply_raw is not null -- Only keep rows where supply was found or carried forward
),

-- Deduplicate by symbol (in case multiple coin types have same symbol)
deduplicated_daily_supply_by_symbol as (
    select
        block_date,
        coin_symbol,
        carried_forward_supply_raw,
        coin_decimals,
        row_number() over (
            partition by block_date, coin_symbol
            order by last_observed_timestamp_ms desc, coin_type desc
        ) as rn
    from daily_supply_with_locf
    qualify rn = 1
),

-- Aggregate total supply by date
aggregated_supply_daily as (
    select
        block_date,
        sum(carried_forward_supply_raw / power(10, coin_decimals)) as total_token_supply,
        -- Create JSON object with supply breakdown by symbol
        map_from_entries(collect_list(
            struct(
                coin_symbol as key,
                cast(round(carried_forward_supply_raw / power(10, coin_decimals), 2) as decimal(38,2)) as value
            )
        )) as supply_breakdown_json,
        count(distinct coin_symbol) as token_count,
        max(last_observed_timestamp_ms) as max_timestamp_ms,
        from_unixtime(max(last_observed_timestamp_ms)/1000) as max_block_time
    from deduplicated_daily_supply_by_symbol
    group by block_date
)

select
    block_date,
    total_token_supply,
    supply_breakdown_json,
    token_count,
    max_block_time as latest_block_time
from aggregated_supply_daily
order by block_date desc 