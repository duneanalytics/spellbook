{{ config(
        schema = 'metrics_ton'
        , alias = 'gas_fees_daily'
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
        date_trunc('day', p.minute) as block_date
        , avg(p.price) as price
    from {{ source('prices', 'usd') }} p
    inner join ton_native_token t
        on p.contract_address = t.contract_address
    where p.blockchain = 'ton'
        group by 1
), fees as (
    -- Low-level fees overview - https://docs.ton.org/v3/documentation/smart-contracts/transaction-fees/fees-low-level
    -- fees paid inside transactions - storage fee, gas fee, compute fee, action fee
    select block_date, sum(
        coalesce(t.total_fees * 1e-9, 0.0) * p.price
    ) as fees
    from
        {{ source('ton', 'transactions') }} t
    join ton_prices p using(block_date)
    where 
        1 = 1
    {% if is_incremental() %}
    and
        {{ incremental_predicate('block_date') }}
    {% endif %}
    group by
        1

    union all

    -- fee paid for sending messages
    select block_date, sum(
        coalesce(m.fwd_fee * 1e-9, 0.0) * p.price
    ) as fees
    from
        {{ source('ton', 'messages') }} m
    join ton_prices p using(block_date)
    where 
        1 = 1
    {% if is_incremental() %}
    and
        {{ incremental_predicate('block_date') }}
    {% endif %}
    group by
        1
)
select 'ton' as blockchain, block_date, sum(fees) as gas_fees_usd
from fees
group by 1, 2

