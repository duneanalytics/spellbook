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

{#-
  Reproduce daily Polymarket CTF balances from ERC1155 transfer events.

  This replaces the previous dependency on tokens_polygon.balances_daily_agg_base
  (which uses a snapshot source that misses some burn/mint transactions and
  carries stale balances). Output schema matches the previous positions input
  so the downstream `positions_raw` model is unchanged in semantics.

  Differences vs. the legacy snapshot-based table observed on backfill:
  - Repro drops "stuck" balances where the snapshot source missed burn events.
  - Repro excludes value=0 no-op transfers that would otherwise create phantom
    `(balance=0, last_updated=day)` rows on days with no real balance change.
-#}

{%- set token_address = '0x4d97dcd97ec945f40cf65f87097ace5ea0476045' %}
{%- set start_date = '2020-09-03' %}
{%- set zero_address = '0x0000000000000000000000000000000000000000' %}
{#- The start of the incremental merge window. Identical expression to the one
    `incremental_predicate('day')` expands to, so we can clip sequence ranges
    to the same bounds the predicate would otherwise filter out. -#}
{%- set incremental_window_start -%}
date_trunc('{{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }}', now() - interval '{{ var("DBT_ENV_INCREMENTAL_TIME") }}' {{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }})
{%- endset %}

with transfers as (
  select
    cast(date_trunc('day', evt_block_time) as date) as day,
    "from",
    "to" as to_address,
    id as token_id,
    cast(value as int256) as amount
  from {{ source('erc1155_polygon', 'evt_TransferSingle') }}
  where contract_address = {{ token_address }}
    and cast(evt_block_time as date) >= date '{{ start_date }}'
    {% if is_incremental() -%}
    and {{ incremental_predicate('evt_block_time') }}
    {%- endif %}

  union all

  select
    cast(date_trunc('day', t.evt_block_time) as date) as day,
    t."from",
    t."to" as to_address,
    u.token_id,
    cast(u.amount as int256) as amount
  from {{ source('erc1155_polygon', 'evt_TransferBatch') }} as t
  cross join unnest(t.ids, t."values") as u(token_id, amount)
  where t.contract_address = {{ token_address }}
    and cast(t.evt_block_time as date) >= date '{{ start_date }}'
    {% if is_incremental() -%}
    and {{ incremental_predicate('t.evt_block_time') }}
    {%- endif %}
),

-- One signed delta row per (address, token_id, day). Mint (from = 0x0) and burn
-- (to = 0x0) sides drop out by not contributing on the 0x0 side. Days where
-- every transfer carries value=0 are filtered out (no-op transfers would
-- otherwise create phantom `balance=0` anchor rows). Intraday round-trips with
-- non-zero amounts that net to zero are kept so the closure marker is emitted.
daily_deltas as (
  select day, address, token_id, sum(delta) as delta_raw
  from (
    select day, to_address as address, token_id, amount as delta
    from transfers
    where to_address <> {{ zero_address }}

    union all

    select day, "from" as address, token_id, -amount as delta
    from transfers
    where "from" <> {{ zero_address }}
  ) flows
  where day < current_date  -- exclude today to avoid mid-day partials
  group by 1, 2, 3
  having max(abs(delta)) > int256 '0'
),

{% if is_incremental() -%}
-- Seed: latest *real change* balance per (address, token_id) from before the
-- incremental window. Aggregate by `last_updated` rather than `day` because
-- {{ this }} carries forward-filled rows where `day` is the fill day and
-- `last_updated` is the underlying change day; using `max(day)` would rewrite
-- `last_updated` for every inactive pair on every incremental run.
prior_balances as (
  -- {{ this }} is a daily forward-filled snapshot, so the latest pre-window `day`
  -- partition already carries the current balance per (address, token_id) with
  -- `last_updated` = its last real-change day. Reading just that partition is
  -- equivalent to max_by(balance_raw, last_updated) over all history: pairs absent
  -- from it are closed positions (balance_raw = 0) whose forward-fill stops before
  -- the window, so they emit no output rows. Collapses a full-history self-scan
  -- (~835 GB, referenced twice) to a single-partition read (~7 GB).
  select
    address,
    token_id,
    last_updated as day,
    balance_raw
  from {{ this }}
  where day = (
    select max(day) from {{ this }} where not {{ incremental_predicate('day') }}
  )
),

window_deltas as (
  select day, address, token_id, delta_raw
  from daily_deltas
  where {{ incremental_predicate('day') }}
),

reconstructed_balances as (
  select
    d.day,
    d.address,
    d.token_id,
    cast(greatest(
      int256 '0',
      coalesce(cast(p.balance_raw as int256), int256 '0')
        + sum(d.delta_raw) over (
            partition by d.address, d.token_id
            order by d.day
            rows between unbounded preceding and current row
          )
    ) as uint256) as balance_raw
  from window_deltas as d
  left join prior_balances as p
    on d.address = p.address
    and d.token_id = p.token_id
),

-- Union: in-window reconstructed balances + prior anchor row per pair (so the
-- forward-fill can cover pairs with no in-window deltas).
balance_changes as (
  select day, address, token_id, balance_raw from reconstructed_balances
  union all
  select day, address, token_id, balance_raw from prior_balances
)

{%- else -%}
balance_changes as (
  select
    day,
    address,
    token_id,
    cast(greatest(int256 '0', sum(delta_raw) over (
      partition by address, token_id
      order by day
      rows between unbounded preceding and current row
    )) as uint256) as balance_raw
  from daily_deltas
)
{%- endif %}
,

-- Forward-fill each balance row to the day before the next balance change.
-- For open positions the fill extends to yesterday. For closed positions the
-- fill stops at the closure day itself, so we never expand a multi-year
-- sequence just to have the final WHERE drop it. In incremental runs the
-- fill_start is also clipped to the window start so cold pairs (anchor far
-- in the past) don't expand the full history before predicate filtering.
forward_fill as (
  select
    expanded_day as day,
    address,
    token_id,
    balance_raw,
    anchor_day as last_updated
  from (
    select
      anchor_day,
      address,
      token_id,
      balance_raw,
      fill_end,
      {% if is_incremental() -%}
      greatest(anchor_day, cast({{ incremental_window_start }} as date)) as fill_start
      {%- else -%}
      anchor_day as fill_start
      {%- endif %}
    from (
      select
        day as anchor_day,
        address,
        token_id,
        balance_raw,
        coalesce(
          date_add('day', -1, cast(lead(day) over (partition by address, token_id order by day) as date)),
          case
            when balance_raw = uint256 '0' then day
            else current_date - interval '1' day
          end
        ) as fill_end
      from balance_changes
    )
  ) bounded
  cross join unnest(sequence(fill_start, fill_end, interval '1' day)) as t(expanded_day)
  where fill_end >= fill_start
)

select
  'polygon' as blockchain,
  day,
  address,
  {{ token_address }} as token_address,
  token_id,
  balance_raw,
  last_updated
from forward_fill
where balance_raw > uint256 '0'
   or (balance_raw = uint256 '0' and last_updated = day)
