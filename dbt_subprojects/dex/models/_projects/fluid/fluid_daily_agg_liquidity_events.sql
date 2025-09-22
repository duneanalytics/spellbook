{{ config(
    schema = 'fluid'
    , alias = 'daily_agg_liquidity_events'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'dex', 'block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    )
}}

with 

enrich_prices as (
    select 
        *
    from 
    {{ ref('fluid_liquidity_events') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

select 
    date_trunc('month', ep.block_time) as block_month
    , date_trunc('day', ep.block_time) as block_date
    , ep.blockchain
    , ep.project
    , ep.version
    , fp.dex 
    , fp.supply_token as token0 
    , fp.borrow_token as token1 
    , fp.supply_token_symbol as token0_symbol 
    , fp.borrow_token_symbol as token1_symbol 
    , sum(case when fp.supply_token = ep.token_address then ep.supply_amount_raw else 0 end) as amount0_raw 
    , sum(case when fp.supply_token = ep.token_address then ep.supply_amount_raw / pow(10, fp.supply_token_decimals) else 0 end) as amount0
    , max(case when fp.supply_token = ep.token_address then ep.supply_exchange_price else 0 end) as amount0_price
    , sum(case when fp.borrow_token = ep.token_address then ep.supply_amount_raw else 0 end) as amount1_raw 
    , sum(case when fp.borrow_token = ep.token_address then ep.supply_amount_raw / pow(10, fp.borrow_token_decimals) else 0 end) as amount1
    , max(case when fp.borrow_token = ep.token_address then ep.supply_exchange_price else 0 end) as amount1_price
from 
enrich_prices ep 
inner join 
{{ ref('fluid_pools') }} fp 
    on ep.blockchain = fp.blockchain 
    and ep.user_address = fp.dex 
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10