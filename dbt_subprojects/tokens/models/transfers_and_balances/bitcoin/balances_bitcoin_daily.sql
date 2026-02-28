{{ config(
    schema = 'balances_bitcoin'
    , alias = 'daily'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , partition_by = ['day']
    , unique_key = ['day', 'wallet_address']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
    , post_hook = '{{ hide_spells() }}'
) }}

-- Follows the forward-fill balance pattern from stablecoins_tron_balances_from_transfers.
-- Adapted for Bitcoin UTXO model: outputs = deposits, inputs = withdrawals.

with

deposits as (
    select
        o.block_date as day
        , o.block_time
        , o.address as wallet_address
        , cast(o.value as double) as inflow
        , 0e0 as outflow
    from {{ source('bitcoin', 'outputs') }} as o
    where o.address is not null
        and o.value > 0
    {% if is_incremental() %}
        and {{ incremental_predicate('o.block_time') }}
    {% endif %}
)

, withdrawals as (
    select
        i.block_date as day
        , i.block_time
        , i.address as wallet_address
        , 0e0 as inflow
        , cast(i.value as double) as outflow
    from {{ source('bitcoin', 'inputs') }} as i
    where i.address is not null
        and i.value > 0
    {% if is_incremental() %}
        and {{ incremental_predicate('i.block_time') }}
    {% endif %}
)

, all_flows as (
    select * from deposits
    union all
    select * from withdrawals
)

, daily_aggregated as (
    select
        f.day
        , f.wallet_address
        , max(f.block_time) as last_updated
        , sum(f.inflow) as daily_inflow
        , sum(f.outflow) as daily_outflow
    from all_flows as f
    group by 1, 2
)

{% if is_incremental() %}
, prior_balances as (
    select
        wallet_address
        , max(day) as day
        , max_by(last_updated, day) as last_updated
        , max_by(balance_satoshi, day) as balance_satoshi
    from {{ this }}
    where not {{ incremental_predicate('day') }}
    group by 1
)
{% endif %}

, changed_balances as (
    select
        day
        , last_updated
        , wallet_address
        , balance_satoshi
        , lead(cast(day as timestamp)) over (
            partition by wallet_address
            order by day
        ) as next_update_day
    from (
        select
            d.day
            , d.last_updated
            , d.wallet_address
            , greatest(0e0,
                {% if is_incremental() %}
                coalesce(p.balance_satoshi, 0e0) +
                {% endif %}
                sum(d.daily_inflow - d.daily_outflow) over (
                    partition by d.wallet_address
                    order by d.day
                    rows between unbounded preceding and current row
                )
            ) as balance_satoshi
        from daily_aggregated as d
        {% if is_incremental() %}
        left join prior_balances as p
            on d.wallet_address = p.wallet_address
        {% endif %}
        {% if is_incremental() %}
        union all
        select
            p.day
            , p.last_updated
            , p.wallet_address
            , p.balance_satoshi
        from prior_balances as p
        {% endif %}
    )
)

, days as (
    select *
    from unnest(
        sequence(
            date '2009-01-03'
            , date(date_trunc('day', now()))
            , interval '1' day
        )
    ) as foo(day)
    {% if is_incremental() %}
    where {{ incremental_predicate('cast(day as timestamp)') }}
    {% endif %}
)

, forward_fill as (
    select
        d.day
        , b.wallet_address
        , b.balance_satoshi
        , b.last_updated
    from days as d
    left join changed_balances as b
        on d.day >= b.day
        and (b.next_update_day is null or cast(d.day as timestamp) < b.next_update_day)
)

select
    'bitcoin' as blockchain
    , f.day
    , f.wallet_address
    , f.balance_satoshi
    , f.balance_satoshi / 1e8 as balance_btc
    , p.price as btc_price_usd
    , f.balance_satoshi / 1e8 * coalesce(p.price, 0) as balance_usd
    , f.last_updated
from forward_fill as f
left join {{ source('prices', 'usd') }} as p
    on f.day = p.minute
    and p.symbol = 'BTC'
    and p.blockchain is null
where f.balance_satoshi > 0
    and f.wallet_address is not null
{% if is_incremental() %}
    and {{ incremental_predicate('f.day') }}
{% endif %}
