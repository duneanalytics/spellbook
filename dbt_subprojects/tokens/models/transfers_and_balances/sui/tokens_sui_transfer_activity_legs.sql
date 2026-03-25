{{
  config(
    schema = 'tokens_sui',
    alias = 'transfer_activity_legs',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date', 'unique_key']
  )
}}

with legs_base as (
  select
    unique_key,
    blockchain,
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
  from {{ ref('tokens_sui_transfer_activity_legs_base') }}
  where 1 = 1
  {% if is_incremental() -%}
    and {{ incremental_predicate('block_time') }}
  {% endif -%}
),

transfers_enriched as (
  select
    unique_key as transfer_unique_key,
    token_standard,
    contract_address,
    symbol,
    amount,
    price_usd,
    amount_usd
  from {{ ref('tokens_sui_transfers') }}
  where 1 = 1
  {% if is_incremental() -%}
    and {{ incremental_predicate('block_time') }}
  {% endif -%}
),

enriched as (
  select
    b.unique_key,
    b.blockchain,
    b.block_month,
    b.block_date,
    b.block_time,
    b.block_number,
    b.tx_hash,
    b.tx_digest,
    b.transfer_unique_key,
    b.tx_from,
    b.tx_index,
    b.coin_type,
    b.transfer_type,
    b."from",
    b.to,
    b.amount_raw,
    b.event_index,
    b.event_sender,
    b.event_package,
    b.event_type,
    b.event_module,
    b.event_name,
    b.event_type_params,
    b.coin_type_hint,
    b.coin_type_in_hint,
    b.coin_type_out_hint,
    b.match_reason,
    b.event_json,
    t.token_standard,
    t.contract_address,
    t.symbol,
    t.amount,
    t.price_usd,
    t.amount_usd,
    case
      when b.match_reason = 'event_in_and_out_coin_match' then 1
      when b.match_reason in ('event_coin_in_match', 'event_coin_out_match') then 2
      when b.match_reason = 'event_coin_hint_match' then 3
      when b.match_reason = 'event_type_param_match' then 4
      else 9
    end as match_priority
  from legs_base b
  inner join transfers_enriched t
    on t.transfer_unique_key = b.transfer_unique_key
),

ranked as (
  select
    *,
    row_number() over (
      partition by transfer_unique_key
      order by match_priority, event_index, event_type
    ) as transfer_match_rank,
    count(*) over (partition by transfer_unique_key) as transfer_match_count,
    count(*) over (partition by tx_digest, event_index, event_type) as event_match_count
  from enriched
)

select
  unique_key,
  blockchain,
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
  token_standard,
  contract_address,
  symbol,
  amount,
  price_usd,
  amount_usd,
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
  event_json,
  match_priority,
  case
    when match_priority = 1 then 'high'
    when match_priority in (2, 3) then 'medium'
    else 'low'
  end as match_confidence,
  transfer_match_rank,
  transfer_match_rank = 1 as is_primary_match,
  transfer_match_count,
  event_match_count,
  cast(1 as double) / nullif(cast(transfer_match_count as double), 0) as allocation_weight,
  amount_usd * (cast(1 as double) / nullif(cast(transfer_match_count as double), 0)) as allocated_amount_usd
from ranked
