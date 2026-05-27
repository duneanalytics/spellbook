with params as (
    select
        cast(current_date - interval '7' day as date) as window_start_day
        , cast(current_date - interval '1' day as date) as window_end_day
)

, expected as (
    select
        p.blockchain
        , p.day
        , p.address
        , p.token_address
        , p.token_id
        , p.balance_raw
    from polymarket_polygon.positions_raw as p
    cross join params as w
    where p.day between w.window_start_day and w.window_end_day
)

, actual as (
    select
        p.blockchain
        , p.day
        , p.address
        , p.token_address
        , p.token_id
        , p.balance_raw
    from {{ ref('polymarket_polygon_positions_raw') }} as p
    cross join params as w
    where p.day between w.window_start_day and w.window_end_day
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
