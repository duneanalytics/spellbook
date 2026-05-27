with params as (
    select
        cast(current_date - interval '7' day as date) as window_start_day
        , cast(current_date - interval '1' day as date) as window_end_day
)

{% set window_start_date = (modules.datetime.date.today() - modules.datetime.timedelta(days=7)).strftime('%Y-%m-%d') %}

, token_list as (
    select
        0x4d97dcd97ec945f40cf65f87097ace5ea0476045 as token_address
)

, changed_pairs as (
    select distinct
        b.address
        , b.token_id
    from {{ source('tokens_polygon', 'balances_daily_agg_base') }} as b
    cross join params as w
    cross join token_list as t
    where b.token_address = t.token_address
      and b.day between w.window_start_day and w.window_end_day
)

, expected_base as (
    {{
      balances_incremental_subset_daily(
            blockchain = 'polygon',
            token_list = 'token_list',
            start_date = window_start_date
      )
    }}
)

, expected as (
    select
        e.blockchain
        , e.day
        , e.address
        , e.token_address
        , e.token_id
        , e.balance_raw
    from expected_base as e
    inner join changed_pairs as cp
        on e.address = cp.address
        and e.token_id = cp.token_id
)

, actual as (
    select
        a.blockchain
        , a.day
        , a.address
        , a.token_address
        , a.token_id
        , a.balance_raw
    from {{ ref('polymarket_polygon_positions_balances_repro') }} as a
    cross join params as w
    inner join changed_pairs as cp
        on a.address = cp.address
        and a.token_id = cp.token_id
    where a.day between w.window_start_day and w.window_end_day
)

, expected_minus_actual as (
    select
        blockchain
        , day
        , address
        , token_address
        , token_id
        , balance_raw
    from expected
    except
    select
        blockchain
        , day
        , address
        , token_address
        , token_id
        , balance_raw
    from actual
)

, actual_minus_expected as (
    select
        blockchain
        , day
        , address
        , token_address
        , token_id
        , balance_raw
    from actual
    except
    select
        blockchain
        , day
        , address
        , token_address
        , token_id
        , balance_raw
    from expected
)

select * from expected_minus_actual
union all
select * from actual_minus_expected
