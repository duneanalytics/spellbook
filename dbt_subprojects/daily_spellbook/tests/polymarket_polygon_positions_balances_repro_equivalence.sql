{% set rolling_window_days = 7 %}
{% set window_start_date = (modules.datetime.date.today() - modules.datetime.timedelta(days=rolling_window_days)).strftime('%Y-%m-%d') %}

with token_list as (
    select
        0x4d97dcd97ec945f40cf65f87097ace5ea0476045 as token_address
)

, expected as (
    {{
      balances_incremental_subset_daily(
            blockchain = 'polygon',
            token_list = 'token_list',
            start_date = window_start_date
      )
    }}
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
    where a.day >= cast('{{ window_start_date }}' as date)
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
