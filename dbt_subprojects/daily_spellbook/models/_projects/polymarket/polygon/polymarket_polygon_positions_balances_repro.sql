{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions_balances_repro',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_address', 'token_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

with constants as (
  select
    cast('2020-09-03' as date) as start_date,
    0x4d97dcd97ec945f40cf65f87097ace5ea0476045 as token_address,
    0x0000000000000000000000000000000000000000 as burn_or_mint_address
)

, transfer_single as (
  select
    t.evt_block_time as block_time
    , t.evt_block_number as block_number
    , t."from"
    , t.to
    , t.id as token_id
    , t.value as amount_raw
  from {{ source('erc1155_polygon', 'evt_TransferSingle') }} as t
  cross join constants as c
  where t.contract_address = c.token_address
    and cast(t.evt_block_time as date) >= c.start_date
    {% if is_incremental() %}
    and {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

, transfer_batch as (
  select
    t.evt_block_time as block_time
    , t.evt_block_number as block_number
    , t."from"
    , t.to
    , a.token_id
    , a.amount_raw
  from {{ source('erc1155_polygon', 'evt_TransferBatch') }} as t
  cross join unnest(t.ids, t."values") as a(token_id, amount_raw)
  cross join constants as c
  where t.contract_address = c.token_address
    and cast(t.evt_block_time as date) >= c.start_date
    {% if is_incremental() %}
    and {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

, transfers as (
  select
    s.block_time
    , s.block_number
    , s."from"
    , s.to
    , s.token_id
    , s.amount_raw
  from transfer_single as s
  union all
  select
    b.block_time
    , b.block_number
    , b."from"
    , b.to
    , b.token_id
    , b.amount_raw
  from transfer_batch as b
)

, daily_flows as (
  select
    cast(date_trunc('day', t.block_time) as date) as day
    , max(t.block_number) as block_number
    , max(t.block_time) as block_time
    , t.to as address
    , t.token_id
    , sum(cast(t.amount_raw as int256)) as delta_raw
  from transfers as t
  cross join constants as c
  where t.to != c.burn_or_mint_address
  group by 1, 4, 5

  union all

  select
    cast(date_trunc('day', t.block_time) as date) as day
    , max(t.block_number) as block_number
    , max(t.block_time) as block_time
    , t."from" as address
    , t.token_id
    , sum((-1) * cast(t.amount_raw as int256)) as delta_raw
  from transfers as t
  cross join constants as c
  where t."from" != c.burn_or_mint_address
  group by 1, 4, 5
)

, daily_deltas as (
  select
    f.day
    , max(f.block_number) as block_number
    , max(f.block_time) as block_time
    , f.address
    , f.token_id
    , sum(f.delta_raw) as delta_raw
  from daily_flows as f
  where f.day < current_date
  group by 1, 4, 5
)

{% if is_incremental() -%}
, prior_balances as (
  select
    b.address
    , b.token_id
    , max(b.day) as day
    , max_by(b.balance_raw, b.day) as balance_raw
  from {{ this }} as b
  where not {{ incremental_predicate('b.day') }}
  group by 1, 2
)

, in_window_deltas as (
  select
    d.day
    , d.block_number
    , d.block_time
    , d.address
    , d.token_id
    , d.delta_raw
  from daily_deltas as d
  where {{ incremental_predicate('d.day') }}
)

, reconstructed_balances as (
  select
    d.day
    , d.block_number
    , d.block_time
    , d.address
    , d.token_id
    , cast(
      greatest(
        0e0,
        least(
          1.0e77,
          coalesce(cast(p.balance_raw as double), 0e0)
            + sum(cast(d.delta_raw as double)) over (
              partition by d.address, d.token_id
              order by d.day
              rows between unbounded preceding and current row
            )
        )
      ) as uint256
    ) as balance_raw
  from in_window_deltas as d
  left join prior_balances as p
    on d.address = p.address
    and d.token_id = p.token_id
)

, changed_balances_base as (
  select
    r.day
    , r.block_number
    , r.block_time
    , r.address
    , r.token_id
    , r.balance_raw
  from reconstructed_balances as r

  union all

  select
    p.day
    , cast(null as bigint) as block_number
    , cast(p.day as timestamp) as block_time
    , p.address
    , p.token_id
    , p.balance_raw
  from prior_balances as p
)

{% else -%}
, changed_balances_base as (
  select
    d.day
    , d.block_number
    , d.block_time
    , d.address
    , d.token_id
    , cast(
      greatest(
        0e0,
        least(
          1.0e77,
          sum(cast(d.delta_raw as double)) over (
            partition by d.address, d.token_id
            order by d.day
            rows between unbounded preceding and current row
          )
        )
      ) as uint256
    ) as balance_raw
  from daily_deltas as d
)

{% endif -%}
, changed_balances as (
  select
    b.day
    , b.address
    , b.token_id
    , b.balance_raw
    , lead(cast(b.day as timestamp)) over (
      partition by b.address, b.token_id
      order by b.day
    ) as next_update_day
  from changed_balances_base as b
)

, forward_fill as (
  select
    expanded_day as day
    , b.address
    , b.token_id
    , b.balance_raw
    , b.day as last_updated
  from changed_balances as b
  cross join unnest(
    sequence(
      b.day
      , coalesce(
          date_add('day', -1, cast(b.next_update_day as date))
          , current_date - interval '1' day
        )
      , interval '1' day
    )
  ) as expanded(expanded_day)
  where coalesce(
      date_add('day', -1, cast(b.next_update_day as date))
      , current_date - interval '1' day
    ) >= b.day
  {% if is_incremental() %}
  and {{ incremental_predicate('expanded_day') }}
  {% endif %}
)

select
  'polygon' as blockchain
  , f.day
  , f.address
  , c.token_address
  , f.token_id
  , f.balance_raw
  , f.last_updated
from forward_fill as f
cross join constants as c
where (
    f.balance_raw > uint256 '0'
    or (f.balance_raw = uint256 '0' and f.last_updated = f.day)
  )
  and f.address is not null
