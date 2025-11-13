{% macro dex_roundtrip_trades(blockchain) %}

with 

dex_trades as (
    select 
        *,
        case 
            when maker is null then project_contract_address
            else maker 
        end as pool_address -- for singletons 
    from 
    {{ ref('dex_trades') }}
    where blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
),

front_back as (
    select 
        distinct 
        front.block_time
        , front.pool_address
        , front.block_date 
        , front.blockchain
        , t.tx_hash_all AS tx_hash
        , front.project
        , front.version
        , front.project_contract_address
        , t.evt_index_all AS evt_index
    from 
    dex_trades front 
    inner join 
    dex_trades back 
        on front.block_date = back.block_date
        and front.project = back.project 
        and front.version = back.version 
        and front.pool_address = back.pool_address 
        and front.block_number = back.block_number
        and front.evt_index < back.evt_index 
        and front.tx_from = back.tx_from
        and front.token_sold_address = back.token_bought_address
        and front.token_bought_address = back.token_sold_address
        and front.token_sold_amount / back.token_bought_amount BETWEEN 0.99 AND 1.01
        and front.token_bought_amount / back.token_sold_amount BETWEEN 0.99 AND 1.01

    cross join UNNEST(ARRAY[(front.tx_hash, front.evt_index), (back.tx_hash, back.evt_index)]) AS t(tx_hash_all, evt_index_all)
    
    where front.token_sold_amount > 0 
    and front.token_bought_amount > 0 
    and back.token_sold_amount > 0 
    and back.token_bought_amount > 0 -- sanity for divison above
)

select
    t.blockchain
    , t.project
    , t.version
    , t.block_time
    , t.block_date
    , t.block_month
    , t.block_number
    , t.token_sold_address
    , t.token_bought_address
    , t.token_sold_symbol
    , t.token_bought_symbol
    , t.maker
    , t.taker
    , t.tx_hash
    , t.tx_from
    , t.tx_to
    , t.project_contract_address
    , t.pool_address
    , t.token_pair
    , t.token_sold_amount_raw
    , t.token_bought_amount_raw
    , t.token_sold_amount
    , t.token_bought_amount
    , t.amount_usd
    , t.evt_index
from 
dex_trades t
inner join 
front_back fb 
    on t.block_date = fb.block_date
    and t.blockchain = fb.blockchain
    and t.block_time = fb.block_time
    and t.tx_hash = fb.tx_hash
    and t.pool_address = fb.pool_address
    and t.evt_index = fb.evt_index
    and t.project = fb.project 

{% endmacro %}