{{
  config(
    schema = 'tokens_sui',
    alias = 'base_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date', 'unique_key']
  )
}}

with txs as (
  select
    t.transaction_digest,
    from_unixtime(t.timestamp_ms / 1000) as block_time,
    cast(date(from_unixtime(t.timestamp_ms / 1000)) as date) as block_date,
    cast(date_trunc('month', from_unixtime(t.timestamp_ms / 1000)) as date) as block_month,
    cast(t.checkpoint as bigint) as block_number,
    t.sender as tx_from,
    cast(null as varbinary) as tx_to,
    cast(t.transaction_position as integer) as tx_index,
    t.gas_owner,
    cast(coalesce(t.total_gas_cost, 0) as decimal(38, 0)) as total_gas_cost
  from {{ source('sui', 'transactions') }} t
  where t.execution_success = true
  {% if is_incremental() -%}
    and {{ incremental_predicate('from_unixtime(t.timestamp_ms / 1000)') }}
  {% endif -%}
),

touched_objects as (
  select
    txs.transaction_digest,
    txs.block_month,
    txs.block_date,
    txs.block_time,
    txs.block_number,
    txs.tx_from,
    txs.tx_to,
    txs.tx_index,
    txs.gas_owner,
    txs.total_gas_cost,
    tobj.object_id,
    try_cast(tobj.version as bigint) as object_version,
    regexp_replace(lower(o.coin_type), '^0x0*([0-9a-f]+)(::.*)$', '0x$1$2') as coin_type,
    o.owner_address,
    o.owner_type,
    cast(o.coin_balance as decimal(38, 0)) as coin_balance,
    o.object_status,
    o.previous_transaction
  from txs
  inner join {{ source('sui', 'transaction_objects') }} tobj
    on tobj.transaction_digest = txs.transaction_digest
    and tobj.date = txs.block_date
  inner join {{ source('sui', 'objects') }} o
    on o.object_id = tobj.object_id
    and o.version = tobj.version
),

coin_states_direct as (
  select
    transaction_digest,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_from,
    tx_to,
    tx_index,
    gas_owner,
    total_gas_cost,
    object_id,
    coin_type,
    owner_address,
    owner_type,
    coin_balance,
    case
      when previous_transaction = transaction_digest then 'post'
      else 'pre'
    end as state
  from touched_objects
  where coin_type is not null
    and owner_address is not null
    and coin_balance is not null
),

deleted_pre_lineage as (
  select
    d.transaction_digest,
    d.block_month,
    d.block_date,
    d.block_time,
    d.block_number,
    d.tx_from,
    d.tx_to,
    d.tx_index,
    d.gas_owner,
    d.total_gas_cost,
    regexp_replace(lower(p.coin_type), '^0x0*([0-9a-f]+)(::.*)$', '0x$1$2') as coin_type,
    p.owner_address,
    p.owner_type,
    cast(p.coin_balance as decimal(38, 0)) as coin_balance,
    'pre' as state
  from (
  select distinct
      t.transaction_digest,
      t.block_month,
      t.block_date,
      t.block_time,
      t.block_number,
      t.tx_from,
      t.tx_to,
      t.tx_index,
      t.gas_owner,
      t.total_gas_cost,
      t.object_id,
      t.object_version as deleted_version
  from touched_objects t
  where t.object_status = 'Deleted'
    and not exists (
      select 1
      from touched_objects p
      where p.transaction_digest = t.transaction_digest
        and p.object_id = t.object_id
        and p.coin_type is not null
        and p.owner_address is not null
        and p.coin_balance is not null
        and case
              when p.previous_transaction = p.transaction_digest then 'post'
              else 'pre'
            end = 'pre'
    )
  ) d
  inner join {{ source('sui', 'objects') }} p
    on p.object_id = d.object_id
    and try_cast(p.version as bigint) = d.deleted_version - 1
  where p.coin_type is not null
    and p.owner_address is not null
    and p.coin_balance is not null
),

coin_states as (
  select
    transaction_digest,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_from,
    tx_to,
    tx_index,
    gas_owner,
    total_gas_cost,
    coin_type,
    owner_address,
    owner_type,
    coin_balance,
    state
  from coin_states_direct
  union all
  select
    transaction_digest,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_from,
    tx_to,
    tx_index,
    gas_owner,
    total_gas_cost,
    coin_type,
    owner_address,
    owner_type,
    coin_balance,
    state
  from deleted_pre_lineage
),

owner_deltas_raw as (
  select
    transaction_digest,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_from,
    tx_to,
    tx_index,
    gas_owner,
    total_gas_cost,
    coin_type,
    owner_address,
    owner_type,
    sum(case when state = 'post' then coin_balance else cast(0 as decimal(38, 0)) end)
      - sum(case when state = 'pre' then coin_balance else cast(0 as decimal(38, 0)) end) as raw_delta
  from coin_states
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
),

owner_deltas as (
  select
    transaction_digest,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_from,
    tx_to,
    tx_index,
    coin_type,
    owner_address,
    owner_type,
    case
      when coin_type = '0x2::sui::sui' and owner_address = gas_owner
        then raw_delta + total_gas_cost
      else raw_delta
    end as delta
  from owner_deltas_raw
),

deltas as (
  select
    transaction_digest,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_from,
    tx_to,
    tx_index,
    coin_type,
    owner_address,
    owner_type,
    delta
  from owner_deltas
  where delta <> 0
),

totals as (
  select
    transaction_digest,
    coin_type,
    sum(case when delta > 0 then delta else cast(0 as decimal(38, 0)) end) as total_pos,
    sum(case when delta < 0 then -delta else cast(0 as decimal(38, 0)) end) as total_neg
  from deltas
  group by 1, 2
),

neg_real as (
  select
    transaction_digest,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_from,
    tx_to,
    tx_index,
    coin_type,
    owner_address as from_address,
    owner_type as from_owner_type,
    -delta as amount_raw,
    cast(0 as integer) as is_synth
  from deltas
  where delta < 0
),

pos_real as (
  select
    transaction_digest,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_from,
    tx_to,
    tx_index,
    coin_type,
    owner_address as to_address,
    owner_type as to_owner_type,
    delta as amount_raw,
    cast(0 as integer) as is_synth
  from deltas
  where delta > 0
),

neg_augmented as (
  select
    transaction_digest,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_from,
    tx_to,
    tx_index,
    coin_type,
    from_address,
    from_owner_type,
    amount_raw,
    is_synth
  from neg_real
  union all
  select
    p.transaction_digest,
    min(p.block_month) as block_month,
    min(p.block_date) as block_date,
    min(p.block_time) as block_time,
    min(p.block_number) as block_number,
    min(p.tx_from) as tx_from,
    min(p.tx_to) as tx_to,
    min(p.tx_index) as tx_index,
    p.coin_type,
    0x0000000000000000000000000000000000000000000000000000000000000000 as from_address,
    'synthetic' as from_owner_type,
    max(t.total_pos - t.total_neg) as amount_raw,
    cast(1 as integer) as is_synth
  from pos_real p
  inner join totals t
    on t.transaction_digest = p.transaction_digest
    and t.coin_type = p.coin_type
  where t.total_pos > t.total_neg
  group by 1, 9
),

pos_augmented as (
  select
    transaction_digest,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_from,
    tx_to,
    tx_index,
    coin_type,
    to_address,
    to_owner_type,
    amount_raw,
    is_synth
  from pos_real
  union all
  select
    n.transaction_digest,
    min(n.block_month) as block_month,
    min(n.block_date) as block_date,
    min(n.block_time) as block_time,
    min(n.block_number) as block_number,
    min(n.tx_from) as tx_from,
    min(n.tx_to) as tx_to,
    min(n.tx_index) as tx_index,
    n.coin_type,
    0x0000000000000000000000000000000000000000000000000000000000000000 as to_address,
    'synthetic' as to_owner_type,
    max(t.total_neg - t.total_pos) as amount_raw,
    cast(1 as integer) as is_synth
  from neg_real n
  inner join totals t
    on t.transaction_digest = n.transaction_digest
    and t.coin_type = n.coin_type
  where t.total_neg > t.total_pos
  group by 1, 9
),

neg_intervals as (
  select
    transaction_digest,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_from,
    tx_to,
    tx_index,
    coin_type,
    from_address,
    from_owner_type,
    amount_raw,
    is_synth,
    sum(amount_raw) over (
      partition by transaction_digest, coin_type
      order by is_synth, from_address
    ) - amount_raw as start_amt,
    sum(amount_raw) over (
      partition by transaction_digest, coin_type
      order by is_synth, from_address
    ) as end_amt
  from neg_augmented
),

pos_intervals as (
  select
    transaction_digest,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_from,
    tx_to,
    tx_index,
    coin_type,
    to_address,
    to_owner_type,
    amount_raw,
    is_synth,
    sum(amount_raw) over (
      partition by transaction_digest, coin_type
      order by is_synth, to_address
    ) - amount_raw as start_amt,
    sum(amount_raw) over (
      partition by transaction_digest, coin_type
      order by is_synth, to_address
    ) as end_amt
  from pos_augmented
),

matched_transfers as (
  select
    n.transaction_digest,
    n.block_month,
    n.block_date,
    n.block_time,
    n.block_number,
    n.tx_from,
    n.tx_to,
    n.tx_index,
    n.coin_type,
    n.from_address as "from",
    n.from_owner_type,
    p.to_address as to,
    p.to_owner_type,
    n.is_synth as from_is_synth,
    p.is_synth as to_is_synth,
    greatest(
      cast(0 as decimal(38, 0)),
      least(n.end_amt, p.end_amt) - greatest(n.start_amt, p.start_amt)
    ) as amount_raw_dec
  from neg_intervals n
  inner join pos_intervals p
    on n.transaction_digest = p.transaction_digest
    and n.coin_type = p.coin_type
    and n.start_amt < p.end_amt
    and p.start_amt < n.end_amt
),

normalized as (
  select
    transaction_digest,
    transaction_digest as tx_digest,
    from_base58(transaction_digest) as tx_hash,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_from,
    tx_to,
    tx_index,
    coin_type,
    case
      when from_is_synth = 1 then 'mint'
      when to_is_synth = 1 then 'burn'
      else 'transfer'
    end as transfer_type,
    from_owner_type,
    to_owner_type,
    "from",
    to,
    case
      when lower(coalesce(from_owner_type, '')) = 'objectowner' then "from"
      else cast(null as varbinary)
    end as from_owner_object_id,
    case
      when lower(coalesce(to_owner_type, '')) = 'objectowner' then to
      else cast(null as varbinary)
    end as to_owner_object_id,
    cast(split_part(coin_type, '::', 1) as varbinary) as contract_address,
    cast(amount_raw_dec as uint256) as amount_raw,
    case
      when coin_type = '0x2::sui::sui' then 'native'
      else 'coin'
    end as token_standard
  from matched_transfers
  where amount_raw_dec > 0
    and "from" <> to
)

select
  {{ dbt_utils.generate_surrogate_key([
    'transaction_digest',
    'coin_type',
    'cast("from" as varchar)',
    'cast(to as varchar)',
    'cast(amount_raw as varchar)'
  ]) }} as unique_key,
  'sui' as blockchain,
  block_month,
  block_date,
  block_time,
  block_number,
  tx_hash,
  tx_digest,
  cast(null as integer) as evt_index,
  cast(null as array(bigint)) as trace_address,
  token_standard,
  tx_from,
  tx_to,
  tx_index,
  transfer_type,
  from_owner_type,
  to_owner_type,
  "from",
  to,
  from_owner_object_id,
  to_owner_object_id,
  contract_address,
  amount_raw,
  coin_type
from normalized
where amount_raw > uint256 '0'
