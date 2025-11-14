{% macro dex_multihop_trades(blockchain) %}

with 

dex_trades as (
    select 
        * 
    from 
    {{ ref('dex_trades') }}
    where blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
),

multi_hops as (
    select 
        tx_hash
        , block_date
        , count(*) as swap_count
        , min(evt_index) as first_trade
        , max(evt_index) as last_trade
    from 
    dex_trades
    group by 1, 2 
    having count(*) >= 3 
),

identify_hops as (
    select 
        dt.*
        , mh.first_trade
        , mh.last_trade
        , case
            when dt.evt_index = mh.first_trade and dt.evt_index != mh.last_trade then 'entry'
            when dt.evt_index > mh.first_trade and dt.evt_index < mh.last_trade then 'intermediate'
            when dt.evt_index = mh.last_trade and dt.evt_index != mh.first_trade then 'end'
            else 'direct' 
        end as multihop_label
        , case 
            when length(maker) = 32 then maker -- univ4 virtual pool id length, also same for euler, swaap
            else project_contract_address  
        end as pool_address -- for singletons
    from 
    dex_trades dt 
    left join 
    multi_hops mh 
        on dt.tx_hash = mh.tx_hash 
        and dt.block_date = mh.block_date
),

agg_data as (
    select 
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number 
        , tx_hash 
        , coalesce(
            token_pair
            , case 
                when coalesce(lower(token_bought_symbol), cast(token_bought_address as varchar))
                > coalesce(lower(token_sold_symbol), cast(token_sold_address as varchar))
                then concat(coalesce(lower(token_sold_symbol), cast(token_sold_address as varchar)), '-', coalesce(lower(token_bought_symbol), cast(token_bought_address as varchar)))
                else concat(coalesce(lower(token_bought_symbol), cast(token_bought_address as varchar)), '-', coalesce(lower(token_sold_symbol), cast(token_sold_address as varchar)))
              end 
        ) as token_pair 
        , pool_address
        , sum(case when multihop_label = 'direct' then 1 else 0 end) as direct_trade_count
        , sum(case when multihop_label = 'entry' then 1 else 0 end) as entry_trade_count
        , sum(case when multihop_label = 'intermediate' then 1 else 0 end) as intermediate_trade_count
        , sum(case when multihop_label = 'end' then 1 else 0 end) as end_trade_count
        , sum(case when multihop_label = 'direct' then amount_usd else 0 end) as direct_trade_vol
        , sum(case when multihop_label = 'entry' then amount_usd else 0 end) as entry_trade_vol
        , sum(case when multihop_label = 'intermediate' then amount_usd else 0 end) as intermediate_trade_vol
        , sum(case when multihop_label = 'end' then amount_usd else 0 end) as end_trade_vol
    from 
    identify_hops
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 
)

    select 
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , tx_hash 
        , token_pair
        , pool_address
        , direct_trade_count + entry_trade_count + intermediate_trade_count + end_trade_count as total_trade_count
        , (
            cast(entry_trade_count AS DOUBLE) +
            cast(intermediate_trade_count AS DOUBLE) +
            cast(end_trade_count AS DOUBLE)
        ) /
          (
            cast(direct_trade_count AS DOUBLE) +
            cast(entry_trade_count AS DOUBLE) +
            cast(intermediate_trade_count AS DOUBLE) +
            cast(end_trade_count AS DOUBLE)
        ) as multihop_trade_count_pct
        , direct_trade_count
        , entry_trade_count
        , intermediate_trade_count
        , end_trade_count
        , direct_trade_vol + entry_trade_vol + intermediate_trade_vol + end_trade_vol as total_trade_vol
        , (entry_trade_vol + intermediate_trade_vol + end_trade_vol)/(direct_trade_vol + entry_trade_vol + intermediate_trade_vol + end_trade_vol) as multihop_trades_vol_pct
        , direct_trade_vol
        , entry_trade_vol
        , intermediate_trade_vol
        , end_trade_vol
    from 
    agg_data

{% endmacro %}
