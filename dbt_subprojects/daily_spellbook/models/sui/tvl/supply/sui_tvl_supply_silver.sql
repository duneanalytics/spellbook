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

-- Silver layer: Simple daily aggregated token supply (following Snowflake pattern)
-- Basic aggregation without complex LOCF logic

with daily_supply as (
    select
        block_date,
        coin_type,
        total_supply,
        -- Get latest supply per token per day
        row_number() over (
            partition by block_date, coin_type
            order by timestamp_ms desc
        ) as rn
    from {{ ref('sui_tvl_supply_bronze') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
    {% endif %}
),

supply_with_metadata as (
    select
        s.block_date,
        s.coin_type,
        s.total_supply,
        c.coin_symbol,
        c.coin_decimals
    from daily_supply s
    left join {{ ref('dex_sui_coin_info') }} c
        on s.coin_type = c.coin_type
    where s.rn = 1
        and s.total_supply > 0
)

select
    block_date,
    sum(total_supply / power(10, coalesce(coin_decimals, 9))) as total_token_supply,
    count(distinct coin_symbol) as token_count
from supply_with_metadata
group by block_date
order by block_date desc 