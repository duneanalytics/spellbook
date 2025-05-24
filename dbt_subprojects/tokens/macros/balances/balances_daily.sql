{%- macro balances_daily(balances_daily_agg, start_date, native_token='ETH') %}

with changed_balances as (
    select
    blockchain
    -- Ensure 'day' from balances_daily_agg is consistently a DATE here if it's not already
    ,cast(day as date) as day  -- Let's be explicit
    ,address
    ,token_symbol
    ,token_address
    ,token_standard
    ,token_id
    ,balance
    -- The LEAD function needs day to be orderable, timestamp was fine, let's keep day as date for consistency
    ,lead(cast(day as date)) over (partition by token_address,address,token_id order by cast(day as date) asc) as next_update_day
    from {{balances_daily_agg}}
)

,days_spine as ( -- Renamed to avoid conflict if 'day' is a column name in source
    select cast(foo.day_val as date) as day_val -- Ensure this is a date
    from unnest(
         sequence(cast('{{start_date}}' as date)
                , date(date_trunc('day',now()))
                , interval '1' day
                )
         ) as foo (day_val) -- aliased the generated column
)

, forward_fill as (
    select
        cb.blockchain
        ,ds.day_val as day -- This is now a DATE
        ,cb.address
        ,cb.token_symbol
        ,cb.token_address
        ,cb.token_standard
        ,cb.token_id
        ,cb.balance
        from days_spine ds
        left join changed_balances cb
            ON  ds.day_val >= cb.day -- date vs date
            and (cb.next_update_day is null OR ds.day_val < cb.next_update_day) -- date vs date
)

, filtered_forward_fill as (
    select * from forward_fill
    where balance > 0
)

-- Final SELECT statement
select
    b.blockchain,
    cast(b.day as timestamp(3)) as day, -- Cast to timestamp for final output as in original plan
    b.address,
    b.token_symbol,
    b.token_address,
    b.token_standard,
    b.token_id,
    b.balance,
    b.balance * p.price as balance_usd
from filtered_forward_fill b
left join {{ source('prices', 'usd_daily') }} p -- Direct join to the daily prices source
    ON cast(b.day as date) = p.day -- Critical: join DATE to DATE
    AND b.blockchain = p.blockchain -- Added for completeness, assuming prices.usd_daily has blockchain
    AND (
        (b.token_standard = 'erc20' AND b.token_address = p.contract_address)
        OR
        (b.token_standard = 'native' AND p.symbol = upper('{{native_token}}') AND p.contract_address IS NULL AND p.blockchain = b.blockchain) -- Assuming native token price is per blockchain
    )
{% endmacro %}
