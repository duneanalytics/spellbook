{{ config(
        schema = 'metrics_ton'
        , alias = 'transactions_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

with ton_native_token as ( -- resolve native token by chain, never by symbol (rename-proof: TON -> GRAM)
    select
        coalesce(token_address, 0x0000000000000000000000000000000000000000) as contract_address
    from {{ source('dune', 'blockchains') }}
    where name = 'ton'
), ton_prices as ( -- get price of native TON for each day to estimate USD value
    select
        date_trunc('day', p.minute) as price_day
        , avg(p.price) as price
    from {{ source('prices', 'usd') }} p
    inner join ton_native_token t
        on p.contract_address = t.contract_address
    where p.blockchain = 'ton'
        group by 1
), jetton_prices as (
    select jp.token_address as jetton_master, jp.timestamp as block_date, avg(price_usd) as price_usd
    from {{ ref('ton_jetton_price_daily') }} jp
    group by 1, 2
),
significant_transactions as (
    -- TON transfers
    select
        M.block_date
        , M.tx_hash
    from
        {{ source('ton', 'messages') }} M
    join {{ source('ton', 'transactions') }} T
        on M.tx_hash = T.hash and M.direction = 'in' and M.block_date = T.block_date
    join ton_prices
        on M.block_date = ton_prices.price_day
    where
        1 = 1
        and value / 1e9 * ton_prices.price > 1 -- 1$ filter
        {% if is_incremental() %}
        and {{ incremental_predicate('M.block_date') }}
        and {{ incremental_predicate('T.block_date') }}
        {% endif %}
    
    union all
    -- Jetton transfers

    select
        block_date
        , J.tx_hash
    from
        {{ source('ton', 'jetton_events') }} J
    join jetton_prices
        using (block_date, jetton_master)
    where
        1 = 1
        and amount * jetton_prices.price_usd > 1 -- 1$ filter
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
)
select 'ton' as blockchain
        , block_date
        , approx_distinct(tx_hash) as tx_count
from significant_transactions
group by 1, 2
