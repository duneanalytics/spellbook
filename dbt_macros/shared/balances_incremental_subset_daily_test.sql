{#  @dev

    @notice experimental test version - more efficient incremental logic
    @notice this macro constructs the address level token balances table for given input table
    @notice aka, you give lists of tokens and/or address, it generates table with daily balances of the address-token pair
    @notice this is the base version - it outputs balance_raw only. token symbols, decimals, and prices should be added in downstream enrichment models.
    
    @notice key optimization: in incremental mode, reads last known state from target table instead of re-scanning all historical source data
    
    @param blockchain               -- blockchain name
    @param address_list             -- must have an address column, can be none if only filtering on tokens
    @param token_list               -- must have a token_address column, can be none if only filtering on tokens
    @param address_token_list       -- for advanced usage, must have both (address, token_address) columns, can be none
    @param start_date               -- the start_date, used to generate the daily timeseries

#}

{%- macro balances_incremental_subset_daily_test(
        blockchain,
        start_date,
        address_list = none,
        token_list = none,
        address_token_list = none
    )
%}

with

-- step 1: get new balance changes from source (filtered to incremental window or full for initial load)
new_balance_changes as (
    select
        b.blockchain,
        b.day,
        b.address,
        b.token_address,
        b.token_standard,
        b.token_id,
        b.balance_raw
    from {{ source('tokens_'~blockchain, 'balances_daily_agg_base') }} b
    {% if address_list is not none %}
    inner join (select distinct address from {{ address_list }}) f1
        on f1.address = b.address
    {% endif %}
    {% if token_list is not none %}
    inner join (select distinct token_address from {{ token_list }}) f2
        on f2.token_address = b.token_address
    {% endif %}
    {% if address_token_list is not none %}
    inner join (select distinct address, token_address from {{ address_token_list }}) f3
        on f3.token_address = b.token_address
        and f3.address = b.address
    {% endif %}
    where b.day >= cast('{{ start_date }}' as date)
    {% if is_incremental() %}
        and {{ incremental_predicate('b.day') }}
    {% endif %}
),

{% if is_incremental() %}
-- step 2 (incremental only): get last known state from target table for each address/token/token_id
-- key optimization: read from already-materialized target instead of re-scanning all historical source data
-- include all combos (not just those with new changes) so they get forward-filled into new days
last_known_state as (
    select
        blockchain,
        max(day) as day,
        address,
        token_address,
        token_standard,
        token_id,
        max_by(balance_raw, day) as balance_raw
    from {{ this }}
    group by 1, 3, 4, 5, 6
),

-- step 3 (incremental only): combine last known state with new changes
-- the lead() window function will correctly order these for forward-fill
all_balance_changes as (
    select * from last_known_state
    union all
    select * from new_balance_changes
),

{% else %}
-- full load: use all balance changes from source
all_balance_changes as (
    select * from new_balance_changes
),
{% endif %}

-- step 4: add next_update_day for forward fill logic
changed_balances as (
    select
        *,
        lead(cast(day as timestamp)) over (
            partition by blockchain, token_address, address, token_id 
            order by day asc
        ) as next_update_day
    from all_balance_changes
),

-- step 5: generate days - only for the relevant window (not all days since start_date)
days as (
    select cast(timestamp as date) as day
    from {{ source('utils', 'days') }}
    where cast(timestamp as date) >= cast('{{ start_date }}' as date)
    and cast(timestamp as date) < current_date
    {% if is_incremental() %}
        and {{ incremental_predicate('cast(timestamp as date)') }}
    {% endif %}
),

-- step 6: forward fill - efficient as we only process the incremental window
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
    inner join changed_balances b
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

{% endmacro %}
