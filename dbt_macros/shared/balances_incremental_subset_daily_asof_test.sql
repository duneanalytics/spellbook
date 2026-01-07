{#  @DEV here

    @NOTICE TEST1: ASOF with cross join approach
    @NOTICE Cross join days × address_tokens, then ASOF lookup for each combination

#}

{%- macro balances_incremental_subset_daily_test1(
        blockchain,
        start_date,
        address_list = none,
        token_list = none,
        address_token_list = none
    )
%}

with

source_balances as (
    select
        b.blockchain,
        b.day,
        b.address,
        b.token_address,
        b.token_standard,
        b.token_id,
        b.balance_raw
    from {{source('tokens_'~blockchain,'balances_daily_agg_base')}} b
    {% if address_list is not none %}
    inner join (select distinct address from {{address_list}}) f1
    on f1.address = b.address
    {% endif %}
    {% if token_list is not none %}
    inner join (select distinct token_address from {{token_list}}) f2
    on f2.token_address = b.token_address
    {% endif %}
    {% if address_token_list is not none %}
    inner join (select distinct address, token_address from {{address_token_list}}) f3
    on f3.token_address = b.token_address
    and f3.address = b.address
    {% endif %}
    where b.day >= cast('{{start_date}}' as date)
),

filtered_daily_agg_balances as (
    select
        blockchain,
        day,
        address,
        token_address,
        token_standard,
        token_id,
        balance_raw
    from source_balances
    {% if is_incremental() %}
    where {{ incremental_predicate('day') }}
    {% endif %}
    {% if is_incremental() %}
    union all
    -- last known balance per (address, token) from before incremental window
    select
        blockchain,
        max(day) as day,
        address,
        token_address,
        token_standard,
        token_id,
        max_by(balance_raw, day) as balance_raw
    from source_balances
    where not {{ incremental_predicate('day') }}
    group by 1, 3, 4, 5, 6
    {% endif %}
),

address_tokens as (
    select distinct
        blockchain,
        address,
        token_address,
        token_standard,
        token_id
    from filtered_daily_agg_balances
),

days as (
    select cast(timestamp as date) as day
    from {{ source('utils', 'days') }}
    where cast(timestamp as date) >= cast('{{start_date}}' as date)
    and cast(timestamp as date) < current_date -- exclude today to avoid mid-day stale data
    {% if is_incremental() %}
        and {{ incremental_predicate('cast(timestamp as date)') }}
    {% endif %}
),

address_token_days as (
    select
        at.blockchain,
        at.address,
        at.token_address,
        at.token_standard,
        at.token_id,
        d.day
    from address_tokens at
    cross join days d
),

forward_fill as (
    select
        atd.blockchain,
        atd.day,
        atd.address,
        atd.token_address,
        atd.token_standard,
        atd.token_id,
        b.balance_raw,
        b.day as last_updated
    from address_token_days atd
    asof left join filtered_daily_agg_balances b
        on b.address = atd.address
        and b.token_address = atd.token_address
        and coalesce(cast(b.token_id as varchar), '') = coalesce(cast(atd.token_id as varchar), '')
        and b.day <= atd.day
)

select
    blockchain,
    day,
    address,
    token_address,
    token_standard,
    token_id,
    balance_raw,
    last_updated
from forward_fill
where balance_raw > 0
{% if is_incremental() %}
    and {{ incremental_predicate('day') }}
{% endif %}

{% endmacro %}


{#  @DEV here

    @NOTICE TEST2: ASOF to compute next_update_day + utils.days expansion
    @NOTICE Uses ASOF self-join to find next balance, then joins utils.days to expand validity period

#}

{%- macro balances_incremental_subset_daily_test2(
        blockchain,
        start_date,
        address_list = none,
        token_list = none,
        address_token_list = none
    )
%}

with

source_balances as (
    select
        b.blockchain,
        b.day,
        b.address,
        b.token_address,
        b.token_standard,
        b.token_id,
        max_by(b.balance_raw, b.block_number) as balance_raw
    from {{source('tokens_'~blockchain,'balances_daily_agg_base')}} b
    {% if address_list is not none %}
    inner join (select distinct address from {{address_list}}) f1
    on f1.address = b.address
    {% endif %}
    {% if token_list is not none %}
    inner join (select distinct token_address from {{token_list}}) f2
    on f2.token_address = b.token_address
    {% endif %}
    {% if address_token_list is not none %}
    inner join (select distinct address, token_address from {{address_token_list}}) f3
    on f3.token_address = b.token_address
    and f3.address = b.address
    {% endif %}
    where b.day >= cast('{{start_date}}' as date)
    group by 1, 2, 3, 4, 5, 6
),

filtered_daily_agg_balances as (
    select * from source_balances
    {% if is_incremental() %}
    where {{ incremental_predicate('day') }}
    {% endif %}
    {% if is_incremental() %}
    union all
    -- last known balance per (address, token) from before incremental window
    select
        blockchain,
        max(day) as day,
        address,
        token_address,
        token_standard,
        token_id,
        max_by(balance_raw, day) as balance_raw
    from source_balances
    where not {{ incremental_predicate('day') }}
    group by 1, 3, 4, 5, 6
    {% endif %}
),

-- use ASOF to find the NEXT balance update for each record (replaces LEAD)
balance_with_validity as (
    select
        b.blockchain,
        b.day,
        b.address,
        b.token_address,
        b.token_standard,
        b.token_id,
        b.balance_raw,
        next_b.day as next_update_day
    from filtered_daily_agg_balances b
    asof left join filtered_daily_agg_balances next_b
        on  b.address = next_b.address
        and b.token_address = next_b.token_address
        and coalesce(cast(b.token_id as varchar), '') = coalesce(cast(next_b.token_id as varchar), '')
        and next_b.day > b.day  -- find FIRST balance AFTER this one
),

days as (
    select cast(timestamp as date) as day
    from {{ source('utils', 'days') }}
    where cast(timestamp as date) >= cast('{{start_date}}' as date)
    and cast(timestamp as date) < current_date -- exclude today to avoid mid-day stale data
    {% if is_incremental() %}
        and {{ incremental_predicate('cast(timestamp as date)') }}
    {% endif %}
),

-- expand each balance to cover all days in its validity period using utils.days
forward_fill as (
    select
        b.blockchain,
        d.day,
        b.address,
        b.token_address,
        b.token_standard,
        b.token_id,
        b.balance_raw,
        b.day as last_updated
    from balance_with_validity b
    inner join days d
        on d.day >= b.day
        and d.day < coalesce(b.next_update_day, current_date)
)

select
    blockchain,
    day,
    address,
    token_address,
    token_standard,
    token_id,
    balance_raw,
    last_updated
from forward_fill
where balance_raw > 0
{% if is_incremental() %}
    and {{ incremental_predicate('day') }}
{% endif %}

{% endmacro %}


{#  @DEV here

    @NOTICE TEST3: ASOF for next_update_day + original range join (hybrid)
    @NOTICE Uses ASOF to compute validity range, then original range join for expansion

#}

{%- macro balances_incremental_subset_daily_test3(
        blockchain,
        start_date,
        address_list = none,
        token_list = none,
        address_token_list = none
    )
%}

with

source_balances as (
    select
        b.blockchain,
        b.day,
        b.address,
        b.token_address,
        b.token_standard,
        b.token_id,
        max_by(b.balance_raw, b.block_number) as balance_raw
    from {{source('tokens_'~blockchain,'balances_daily_agg_base')}} b
    {% if address_list is not none %}
    inner join (select distinct address from {{address_list}}) f1
    on f1.address = b.address
    {% endif %}
    {% if token_list is not none %}
    inner join (select distinct token_address from {{token_list}}) f2
    on f2.token_address = b.token_address
    {% endif %}
    {% if address_token_list is not none %}
    inner join (select distinct address, token_address from {{address_token_list}}) f3
    on f3.token_address = b.token_address
    and f3.address = b.address
    {% endif %}
    where b.day >= cast('{{start_date}}' as date)
    group by 1, 2, 3, 4, 5, 6
),

filtered_daily_agg_balances as (
    select * from source_balances
    {% if is_incremental() %}
    where {{ incremental_predicate('day') }}
    {% endif %}
    {% if is_incremental() %}
    union all
    -- last known balance per (address, token) from before incremental window
    select
        blockchain,
        max(day) as day,
        address,
        token_address,
        token_standard,
        token_id,
        max_by(balance_raw, day) as balance_raw
    from source_balances
    where not {{ incremental_predicate('day') }}
    group by 1, 3, 4, 5, 6
    {% endif %}
),

-- use ASOF to compute next_update_day (replaces LEAD window function)
changed_balances as (
    select
        b.blockchain,
        b.day,
        b.address,
        b.token_address,
        b.token_standard,
        b.token_id,
        b.balance_raw,
        next_b.day as next_update_day
    from filtered_daily_agg_balances b
    asof left join filtered_daily_agg_balances next_b
        on  b.address = next_b.address
        and b.token_address = next_b.token_address
        and coalesce(cast(b.token_id as varchar), '') = coalesce(cast(next_b.token_id as varchar), '')
        and next_b.day > b.day
),

days as (
    select cast(timestamp as date) as day
    from {{ source('utils', 'days') }}
    where cast(timestamp as date) >= cast('{{start_date}}' as date)
    and cast(timestamp as date) < current_date -- exclude today to avoid mid-day stale data
    {% if is_incremental() %}
        and {{ incremental_predicate('cast(timestamp as date)') }}
    {% endif %}
),

-- use original range join pattern for forward fill
forward_fill as (
    select
        b.blockchain,
        d.day,
        b.address,
        b.token_address,
        b.token_standard,
        b.token_id,
        b.balance_raw,
        b.day as last_updated
    from days d
    left join changed_balances b
        on d.day >= b.day
        and (b.next_update_day is null or d.day < b.next_update_day)
)

select
    blockchain,
    day,
    address,
    token_address,
    token_standard,
    token_id,
    balance_raw,
    last_updated
from forward_fill
where balance_raw > 0
{% if is_incremental() %}
    and {{ incremental_predicate('day') }}
{% endif %}

{% endmacro %}
