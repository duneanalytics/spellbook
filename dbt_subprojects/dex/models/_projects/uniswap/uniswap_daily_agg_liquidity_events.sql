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
    , sum(amount0_raw) as amount0_raw 
    , sum(amount1_raw) as amount1_raw 
    , sum(amount0) as amount0 
    , sum(amount1) as amount1 
from 
{{ ref('uniswap_liquidity_events') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_date') }}
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
-- comment to refresh
