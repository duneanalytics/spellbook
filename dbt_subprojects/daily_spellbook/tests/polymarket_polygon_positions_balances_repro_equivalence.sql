with token_list as (
    select
        0x4d97dcd97ec945f40cf65f87097ace5ea0476045 as token_address
)

, expected as (
    {{
      balances_incremental_subset_daily(
            blockchain = 'polygon',
            token_list = 'token_list',
            start_date = '2020-09-03'
      )
    }}
)

, actual as (
    select
        blockchain
        , day
        , address
        , token_address
        , token_id
        , balance_raw
        , last_updated
    from {{ ref('polymarket_polygon_positions_balances_repro') }}
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

select *
from expected_minus_actual

union all

select *
from actual_minus_expected
