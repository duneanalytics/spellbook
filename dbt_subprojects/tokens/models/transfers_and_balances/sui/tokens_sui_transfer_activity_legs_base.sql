{{
  config(
    schema = 'tokens_sui',
    alias = 'transfer_activity_legs_base',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date', 'unique_key']
  )
}}

with transfers as (
  select
    unique_key as transfer_unique_key,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_hash,
    tx_digest,
    tx_from,
    tx_index,
    coin_type,
    transfer_type,
    "from",
    to,
    amount_raw
  from {{ ref('tokens_sui_base_transfers') }}
  where 1 = 1
  {% if is_incremental() -%}
    and {{ incremental_predicate('block_time') }}
  {% endif -%}
),

events as (
  select
    e.transaction_digest as tx_digest,
    from_base58(e.transaction_digest) as tx_hash,
    from_unixtime(e.timestamp_ms / 1000) as block_time,
    cast(date(from_unixtime(e.timestamp_ms / 1000)) as date) as block_date,
    cast(date_trunc('month', from_unixtime(e.timestamp_ms / 1000)) as date) as block_month,
    cast(e.checkpoint as bigint) as block_number,
    cast(e.event_index as integer) as event_index,
    e.sender as event_sender,
    e.package as event_package,
    cast(e.event_type as varchar) as event_type,
    cast(e.event_json as varchar) as event_json,
    split_part(cast(e.event_type as varchar), '::', 2) as event_module,
    regexp_replace(split_part(cast(e.event_type as varchar), '::', 3), '<.*$', '') as event_name,
    regexp_extract(cast(e.event_type as varchar), '<(.*)>', 1) as event_type_params,
    regexp_replace(
      lower(coalesce(
        json_extract_scalar(e.event_json, '$.coin_type'),
        json_extract_scalar(e.event_json, '$.coinType'),
        regexp_extract(cast(e.event_type as varchar), '<([^,>]+)>', 1)
      )),
      '^0x0*([0-9a-f]+)(::.*)$',
      '0x$1$2'
    ) as coin_type_hint,
    regexp_replace(
      lower(coalesce(
        json_extract_scalar(e.event_json, '$.coin_type_in'),
        json_extract_scalar(e.event_json, '$.coin_in'),
        json_extract_scalar(e.event_json, '$.coin_x')
      )),
      '^0x0*([0-9a-f]+)(::.*)$',
      '0x$1$2'
    ) as coin_type_in_hint,
    regexp_replace(
      lower(coalesce(
        json_extract_scalar(e.event_json, '$.coin_type_out'),
        json_extract_scalar(e.event_json, '$.coin_out'),
        json_extract_scalar(e.event_json, '$.coin_y')
      )),
      '^0x0*([0-9a-f]+)(::.*)$',
      '0x$1$2'
    ) as coin_type_out_hint
  from {{ source('sui', 'events') }} e
  where 1 = 1
  {% if is_incremental() -%}
    and {{ incremental_predicate('from_unixtime(e.timestamp_ms / 1000)') }}
  {% endif -%}
),

matched as (
  select
    t.transfer_unique_key,
    t.block_month,
    t.block_date,
    t.block_time,
    t.block_number,
    t.tx_hash,
    t.tx_digest,
    t.tx_from,
    t.tx_index,
    t.coin_type,
    t.transfer_type,
    t."from",
    t.to,
    t.amount_raw,
    e.event_index,
    e.event_sender,
    e.event_package,
    e.event_type,
    e.event_module,
    e.event_name,
    e.event_type_params,
    e.event_json,
    e.coin_type_hint,
    e.coin_type_in_hint,
    e.coin_type_out_hint,
    case
      when e.coin_type_in_hint = t.coin_type and e.coin_type_out_hint = t.coin_type then 'event_in_and_out_coin_match'
      when e.coin_type_in_hint = t.coin_type then 'event_coin_in_match'
      when e.coin_type_out_hint = t.coin_type then 'event_coin_out_match'
      when e.coin_type_hint = t.coin_type then 'event_coin_hint_match'
      when e.event_type_params like concat('%', t.coin_type, '%') then 'event_type_param_match'
    end as match_reason
  from transfers t
  inner join events e
    on e.tx_digest = t.tx_digest
    and (e.coin_type_hint = t.coin_type
      or e.coin_type_in_hint = t.coin_type
      or e.coin_type_out_hint = t.coin_type
      or e.event_type_params like concat('%', t.coin_type, '%')
    )
)

select
  {{ dbt_utils.generate_surrogate_key([
    'transfer_unique_key',
    'cast(event_index as varchar)',
    'event_type',
    "coalesce(match_reason, 'unknown')"
  ]) }} as unique_key,
  'sui' as blockchain,
  block_month,
  block_date,
  block_time,
  block_number,
  tx_hash,
  tx_digest,
  transfer_unique_key,
  tx_from,
  tx_index,
  coin_type,
  transfer_type,
  "from",
  to,
  amount_raw,
  event_index,
  event_sender,
  event_package,
  event_type,
  event_module,
  event_name,
  event_type_params,
  coin_type_hint,
  coin_type_in_hint,
  coin_type_out_hint,
  match_reason,
  event_json
from matched
