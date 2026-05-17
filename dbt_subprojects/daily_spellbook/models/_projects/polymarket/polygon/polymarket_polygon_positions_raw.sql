{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions_raw',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_address', 'token_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

with new_changes as (
  select
    blockchain,
    day,
    address,
    token_address,
    token_id,
    balance_raw
  from {{ ref('polymarket_polygon_positions_raw_changes') }}
  where day >= cast('2020-09-03' as date)
    and day < current_date
  {% if is_incremental() %}
    and {{ incremental_predicate('day') }}
  {% endif %}
)

{% if is_incremental() %}
-- Hardcoded day so Trino partition-prunes to a single partition.
, carry_forward as (
  select
    blockchain,
    address,
    token_address,
    token_id,
    balance_raw,
    last_updated
  from {{ this }}
  where day = date_add('day', -1,
    cast(
      date_trunc(
        '{{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }}',
        now() - interval '{{ var("DBT_ENV_INCREMENTAL_TIME") }}' {{ var('DBT_ENV_INCREMENTAL_TIME_UNIT') }}
      ) as date
    )
  )
    and balance_raw > 0
)
{% endif %}

, all_events as (
  select
    blockchain,
    day,
    address,
    token_address,
    token_id,
    balance_raw,
    day as last_updated,
    0 as is_anchor
  from new_changes

  {% if is_incremental() %}
  union all
  select
    blockchain,
    cast(
      date_trunc(
        '{{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }}',
        now() - interval '{{ var("DBT_ENV_INCREMENTAL_TIME") }}' {{ var('DBT_ENV_INCREMENTAL_TIME_UNIT') }}
      ) as date
    ) as day,
    address,
    token_address,
    token_id,
    balance_raw,
    last_updated,
    1 as is_anchor
  from carry_forward
  {% endif %}
)

-- Real change wins over anchor at same (day, address, token_address, token_id).
, deduped as (
  select blockchain, day, address, token_address, token_id, balance_raw, last_updated
  from (
    select
      *,
      row_number() over (
        partition by day, address, token_address, token_id
        order by is_anchor asc
      ) as rn
    from all_events
  )
  where rn = 1
)

, with_next as (
  select
    blockchain,
    day,
    address,
    token_address,
    token_id,
    balance_raw,
    last_updated,
    lead(day) over (
      partition by address, token_address, token_id
      order by day asc
    ) as next_change_day
  from deduped
)

-- Zero-balance change emits only the change day (no forward-fill); greatest() guards stop < start.
, expanded as (
  select
    w.blockchain,
    t.expanded_day as day,
    w.address,
    w.token_address,
    w.token_id,
    w.balance_raw,
    w.last_updated
  from with_next w
  cross join unnest(
    sequence(
      w.day,
      case
        when w.balance_raw = uint256 '0' then w.day
        else greatest(
          w.day,
          coalesce(
            date_add('day', -1, w.next_change_day),
            date_add('day', -1, current_date)
          )
        )
      end,
      interval '1' day
    )
  ) as t(expanded_day)
)

select
  blockchain,
  day,
  address,
  token_address,
  token_id,
  balance_raw,
  balance_raw / 1e6 as balance,
  last_updated
from expanded
{% if is_incremental() %}
where {{ incremental_predicate('day') }}
{% endif %}
