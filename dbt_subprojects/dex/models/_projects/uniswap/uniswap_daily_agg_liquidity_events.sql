{{ config(
    schema = 'uniswap'
    , alias = 'daily_agg_liquidity_events'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'id', 'block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    )
}}

select 
    date_trunc('month', block_time) as block_month
    , date_trunc('day', block_time) as block_date
    , blockchain
    , project
    , version
    , id
    , token0 
    , token1 
    , token0_symbol 
    , token1_symbol 
    , sum(case when event_type = 'fees_accrued' then 0 else amount0_raw end) as amount0_raw
    , sum(case when event_type = 'fees_accrued' then 0 else amount1_raw end) as amount1_raw
    , sum(case when event_type = 'fees_accrued' then 0 else amount0 end) as amount0 
    , sum(case when event_type = 'fees_accrued' then 0 else amount1 end) as amount1
from 
{{ ref('uniswap_liquidity_events') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_date') }}
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- refresh