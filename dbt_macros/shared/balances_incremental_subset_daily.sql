{#  @DEV here

    @NOTICE this macro constructs the address level token balances table for given input table
    @NOTICE aka, you give lists of tokens and/or address, it generates table with daily balances of the address-token pair
    
    @WARN this macro has a dependancy on erc20.tokens. 
    @WARN if your token is not in the default list, manually add it via spellbook/dbt_subprojects/tokens/models/tokens/<chain>/tokens_<chain>_erc20.sql

    @PARAM blockchain               -- blockchain name
    @PARAM address_list             -- must have an address column, can be none if only filtering on tokens
    @PARAM token_list               -- must have a token_address column, can be none if only filtering on tokens
    @PARAM address_token_list       -- for advanced usage, must have both (address, token_address) columns, can be none
    @PARAM start_date               -- the start_date, used to generate the daily timeseries

#}

{%- macro balances_incremental_subset_daily_legacy(
        blockchain,
        start_date,
        address_list = none,
        token_list = none,
        address_token_list = none,
        self_seed_relation = none,
        self_seed_lookback_days = 21,
        apply_ci_floor = true
    )
%}

WITH
filtered_daily_agg_balances as (
    select
        b.blockchain,
        b.day,
        b.block_number,
        b.block_time,
        b.address,
        b.token_address,
        b.token_standard,
        b.balance_raw,
        CASE
            WHEN b.token_standard = 'erc20' THEN b.balance_raw / power(10, erc20_tokens.decimals)
            WHEN b.token_standard = 'native' THEN b.balance_raw / power(10, 18)
            ELSE b.balance_raw
        END as balance,
        erc20_tokens.symbol as token_symbol,
        token_id
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
    left join {{ source('tokens', 'erc20') }} erc20_tokens on
        erc20_tokens.blockchain = '{{blockchain}}'
        AND erc20_tokens.contract_address = b.token_address
        AND b.token_standard = 'erc20'
    where day >= cast('{{start_date}}' as date)
    {% if target.name == 'ci' and apply_ci_floor %}
    -- bound the CI full-refresh build so it completes under the 90-min timeout and the regression test runs; inert in prod.
    -- callers with a check_seed test over fixed historical dates (e.g. swell) must set apply_ci_floor=false so the seeded days build.
    and day >= cast(date_trunc('day', current_date - interval '30' day) as date)
    {% endif %}

)
,changed_balances as (
    select *
     , lead(cast(day as timestamp)) over (partition by token_address,address,token_id order by day asc) as next_update_day
    from (
    select * from (
        select
            blockchain
            ,day
            ,address
            ,token_symbol
            ,token_address
            ,token_standard
            ,token_id
            ,balance
        from filtered_daily_agg_balances
        where day >= cast('{{start_date}}' as date)
        {% if is_incremental() %}
            and {{ incremental_predicate('day') }}
        {% endif %}
    )
    -- if we're running incremental, we need to retrieve the last known balance updates from before the current window
    -- so we can correctly populate the forward fill.
    {% if is_incremental() %}
    UNION ALL
    select * from (
        select
            blockchain
            ,max(day) as day
            ,address
            ,token_symbol
            ,token_address
            ,token_standard
            ,token_id
            ,max_by(balance, day) as balance
        from (
            {% if self_seed_relation is none %}
            -- legacy: re-derive the prior state from all pre-window source history
            select
                blockchain, day, address, token_symbol, token_address, token_standard, token_id, balance
            from filtered_daily_agg_balances
            where day >= cast('{{start_date}}' as date)
            and not {{ incremental_predicate('day') }}
            {% else %}
            -- bounded-lookback hybrid: re-read only the recent pre-window source so late-arriving /
            -- restated balances within the lookback are still self-corrected (these rows carry the
            -- CURRENT token metadata via filtered_daily_agg_balances) ...
            select
                blockchain, day, address, token_symbol, token_address, token_standard, token_id, balance
            from filtered_daily_agg_balances
            where not {{ incremental_predicate('day') }}
            and day >= cast(date_trunc('day', now() - interval '{{ self_seed_lookback_days }}' day) as date)
            union all
            -- ... and carry deep (settled) history forward from the model's own latest pre-window
            -- partition instead of re-scanning the full source history every run. The partition is
            -- anchored strictly before the incremental window (`not incremental_predicate(day)` in
            -- addition to the lookback floor) so a wider repair/backfill where the incremental window
            -- exceeds the lookback can never overlap the in-window source branch above (which would
            -- duplicate a key/day). NB: these carried rows reuse the balance/token_symbol stored when
            -- they were last written; a token-metadata/decimals correction for a position untouched
            -- for >lookback days is only reflected after it next changes or a --full-refresh.
            select
                blockchain, last_updated as day, address, token_symbol, token_address, token_standard, token_id, balance
            from {{ self_seed_relation }}
            where day = (
                select max(day) from {{ self_seed_relation }}
                where day < cast(date_trunc('day', now() - interval '{{ self_seed_lookback_days }}' day) as date)
                and not {{ incremental_predicate('day') }}
            )
            {% endif %}
        )
        group by 1,3,4,5,6,7
        )
    {% endif %}
    )
),

days as (
    select *
    from unnest(
         sequence(cast('{{start_date}}' as date)
                , date(date_trunc('day',now()))
                , interval '1' day
                )
         ) as foo(day)
),
forward_fill as (
    select
        blockchain,
        d.day,
        address,
        token_symbol,
        token_address,
        token_standard,
        token_id,
        balance,
        b.day as last_updated,
        b.next_update_day as next_update
    from days d
        left join changed_balances b
            ON  d.day >= b.day
            and (b.next_update_day is null OR d.day < b.next_update_day) -- perform forward fill
)

select
    b.blockchain,
    b.day,
    b.address,
    b.token_symbol,
    b.token_address,
    b.token_standard,
    b.token_id,
    b.balance,
    b.balance * p.price as balance_usd,
    b.last_updated
from(
    select * from forward_fill
    where balance > 0
    {% if is_incremental() %}
        and {{ incremental_predicate('day') }}
    {% endif %}

) b
left join {{source('prices','usd_daily')}} p
    on 1=1
    {% if is_incremental() %}
    and {{ incremental_predicate('p.day') }}
    {% endif %}
    and ((token_standard = 'erc20'
        and p.blockchain = '{{blockchain}}'
        and b.token_address = p.contract_address
        and b.day = p.day)
    or (token_standard = 'native'
        and p.blockchain is null
        and p.contract_address is null
        and p.symbol = (select native_token_symbol from {{source('evms','info')}} where blockchain = '{{blockchain}}')
        and b.day = p.day))
{% endmacro %}


{#  @DEV here

    @NOTICE this macro constructs the address level token balances table for given input table
    @NOTICE aka, you give lists of tokens and/or address, it generates table with daily balances of the address-token pair
    @NOTICE this is the BASE version - it outputs balance_raw only. Token symbols, decimals, and prices should be added in downstream enrichment models.
    
    @PARAM blockchain               -- blockchain name
    @PARAM address_list             -- must have an address column, can be none if only filtering on tokens
    @PARAM token_list               -- must have a token_address column, can be none if only filtering on tokens
    @PARAM address_token_list       -- for advanced usage, must have both (address, token_address) columns, can be none
    @PARAM start_date               -- the start_date, used to generate the daily timeseries

#}

{%- macro balances_incremental_subset_daily(
        blockchain,
        start_date,
        address_list = none,
        token_list = none,
        address_token_list = none
    )
%}

with

filtered_daily_agg_balances as (
    select
        b.blockchain,
        b.day,
        b.block_number,
        b.block_time,
        b.address,
        b.token_address,
        b.token_standard,
        b.balance_raw,
        b.token_id
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
    where day >= cast('{{start_date}}' as date)

),

changed_balances as (
    select *
     , lead(cast(day as timestamp)) over (partition by token_address,address,token_id order by day asc) as next_update_day
    from (
    select * from (
        select
            blockchain
            ,day
            ,address
            ,token_address
            ,token_standard
            ,token_id
            ,balance_raw
        from filtered_daily_agg_balances
        where day >= cast('{{start_date}}' as date)
        {% if is_incremental() %}
            and {{ incremental_predicate('day') }}
        {% endif %}
    )
    -- if we're running incremental, we need to retrieve the last known balance updates from before the current window
    -- so we can correctly populate the forward fill.
    {% if is_incremental() %}
    union all
    select * from (
        select
            blockchain
            ,max(day) as day
            ,address
            ,token_address
            ,token_standard
            ,token_id
            ,max_by(balance_raw, day) as balance_raw
        from filtered_daily_agg_balances
        where day >= cast('{{start_date}}' as date)
        and not {{ incremental_predicate('day') }}
        group by 1,3,4,5,6
        )
    {% endif %}
    )
),

days as (
    select cast(timestamp as date) as day
    from {{ source('utils', 'days') }}
    where cast(timestamp as date) >= cast('{{start_date}}' as date)
    and cast(timestamp as date) < current_date -- exclude today to avoid mid-day stale data
),

forward_fill as (
    select
        blockchain,
        d.day,
        address,
        token_address,
        token_standard,
        token_id,
        balance_raw,
        b.day as last_updated,
        b.next_update_day as next_update
    from days d
        left join changed_balances b
            on  d.day >= b.day
            and (b.next_update_day is null or d.day < b.next_update_day) -- perform forward fill
)

select
    b.blockchain,
    b.day,
    b.address,
    b.token_address,
    b.token_standard,
    b.token_id,
    b.balance_raw,
    b.last_updated
from (
    select * from forward_fill
    where (balance_raw > 0
       or (balance_raw = 0 and last_updated = day))  -- include actual 0-balance changes, not forward-fills
    {% if is_incremental() %}
        and {{ incremental_predicate('day') }}
    {% endif %}
) b

{% endmacro %}


