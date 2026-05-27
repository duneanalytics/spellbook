with params as (
    select
        0x4d97dcd97ec945f40cf65f87097ace5ea0476045 as token_address
        , cast(current_date - interval '7' day as date) as window_start_day
        , cast(current_date - interval '1' day as date) as window_end_day
)

, balance_updates as (
    select
        b.blockchain
        , b.day
        , b.address
        , b.token_address
        , b.token_id
        , b.balance_raw
    from {{ source('tokens_polygon', 'balances_daily_agg_base') }} as b
    cross join params as p
    where b.token_address = p.token_address
      and b.day <= p.window_end_day
)

, latest_pre_window as (
    select
        b.blockchain
        , max(b.day) as day
        , b.address
        , b.token_address
        , b.token_id
        , max_by(b.balance_raw, b.day) as balance_raw
    from balance_updates as b
    cross join params as p
    where b.day < p.window_start_day
    group by 1, 3, 4, 5
)

, window_updates as (
    select
        b.blockchain
        , b.day
        , b.address
        , b.token_address
        , b.token_id
        , b.balance_raw
    from balance_updates as b
    cross join params as p
    where b.day >= p.window_start_day
)

, changed_balances as (
    select
        cb.blockchain
        , cb.day
        , cb.address
        , cb.token_address
        , cb.token_id
        , cb.balance_raw
        , lead(cast(cb.day as timestamp)) over (
            partition by cb.address, cb.token_address, cb.token_id
            order by cb.day
        ) as next_update_day
    from (
        select * from window_updates
        union all
        select * from latest_pre_window
    ) as cb
)

, days as (
    select
        cast(d.timestamp as date) as day
    from {{ source('utils', 'days') }} as d
    cross join params as p
    where cast(d.timestamp as date) between p.window_start_day and p.window_end_day
)

, expected as (
    select
        cb.blockchain
        , d.day
        , cb.address
        , cb.token_address
        , cb.token_id
        , cb.balance_raw
        , cb.day as last_updated
    from days as d
    inner join changed_balances as cb
        on d.day >= cb.day
        and (
            cb.next_update_day is null
            or cast(d.day as timestamp) < cb.next_update_day
        )
    where (
        cb.balance_raw > uint256 '0'
        or (cb.balance_raw = uint256 '0' and cb.day = d.day)
    )
)

, actual as (
    select
        a.blockchain
        , a.day
        , a.address
        , a.token_address
        , a.token_id
        , a.balance_raw
        , a.last_updated
    from {{ ref('polymarket_polygon_positions_balances_repro') }} as a
    cross join params as p
    where a.day between p.window_start_day and p.window_end_day
)

, expected_minus_actual as (
    select
        blockchain
        , day
        , address
        , token_address
        , token_id
        , balance_raw
        , last_updated
    from expected
    except
    select
        blockchain
        , day
        , address
        , token_address
        , token_id
        , balance_raw
        , last_updated
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
        , last_updated
    from actual
    except
    select
        blockchain
        , day
        , address
        , token_address
        , token_id
        , balance_raw
        , last_updated
    from expected
)

select * from expected_minus_actual
union all
select * from actual_minus_expected
