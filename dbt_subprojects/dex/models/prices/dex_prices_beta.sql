{{ config(
    schema = 'dex'
    , alias = 'prices_beta'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
)
}}

with dex_trades as (
    select distinct
        blockchain
        , block_time
        , token_bought_address
        , token_bought_amount_raw
        , token_bought_amount
        , token_sold_address
        , token_sold_amount_raw
        , token_sold_amount
        , amount_usd
    from
        {{ ref('dex_trades') }}
    where
        1 = 1
        and amount_usd > 0
        {% if is_incremental() %}
        and block_time > (select max(block_time) from {{ this }})
        {% else %}
        and block_date >= date '2024-08-01'
        {% endif %}
),
dex_bought as (
    select
        d.blockchain
        , d.token_bought_address as contract_address
        , t.symbol as symbol
        , t.decimals as decimals
        , d.block_time as block_time
        , d.token_bought_amount as amount
        , coalesce(d.amount_usd/d.token_bought_amount, d.amount_usd/(d.token_bought_amount_raw/POW(10, t.decimals))) as price
    from
        dex_trades as d
    inner join {{ source('tokens', 'erc20') }} as t
        on d.blockchain = t.blockchain
        and d.token_bought_address = t.contract_address
    where
        1 = 1
        and d.token_bought_amount > 0
        and d.token_bought_amount_raw > UINT256 '0'
        and t.symbol is not null
), 
dex_sold as (
    select
        d.blockchain
        , d.token_sold_address as contract_address
        , t.symbol as symbol
        , t.decimals as decimals
        , d.block_time as block_time
        , d.token_sold_amount as amount
        , coalesce(d.amount_usd/d.token_sold_amount, d.amount_usd/(d.token_sold_amount_raw/POW(10, t.decimals))) as price
    from
        dex_trades as d
    inner join {{ source('tokens', 'erc20') }} as t
        on d.blockchain = t.blockchain
        and d.token_sold_address = t.contract_address
    where
        1 = 1
        and d.token_sold_amount > 0
        and d.token_sold_amount_raw > UINT256 '0'
        and t.symbol is not null
),
dex_prices as (
    select
        *
    from
        dex_bought
    union
    select
        *
    from
        dex_sold
)
select
    blockchain
    , contract_address
    , symbol
    , decimals
    , cast(date_trunc('month', block_time) as date) as block_month -- for partitioning
    , block_time
    , amount
    , price
from
    dex_prices