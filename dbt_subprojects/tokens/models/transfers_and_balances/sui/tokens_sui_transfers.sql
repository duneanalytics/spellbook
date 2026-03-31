{{
  config(
    schema = 'tokens_sui',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_date'],
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
    post_hook = '{{ hide_spells() }}',
  )
}}

-- temp filter to unblock ci run (original start date '2023-04-12')
{% set sui_transfer_start_date = '2026-01-01' %}

with

base_transfers as (
  select
    t.*,
    regexp_replace(
      lower(t.coin_type),
      '^0x0*([0-9a-f]+)(::.*)$',
      '0x$1$2'
    ) as coin_type_normalized
  from {{ ref('tokens_sui_base_transfers') }} t
  where t.block_date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('t.block_date') }}
    {% endif %}
),

prices as (
  select
    blockchain,
    timestamp,
    regexp_replace(
      lower(from_utf8(contract_address)),
      '^0x0*([0-9a-f]+)$',
      '0x$1'
    ) as contract_address,
    decimals,
    symbol,
    price
  from {{ source('prices_external', 'hour') }}
  where blockchain = 'sui'
    and timestamp >= timestamp '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('timestamp') }}
    {% endif %}
),

coin_metadata as (
  select
    m.coin_type,
    m.symbol,
    m.decimals
  from {{ ref('sui_coin_info') }} m
),

trusted_tokens as (
  select
    regexp_replace(
      lower(from_utf8(contract_address)),
      '^0x0*([0-9a-f]+)$',
      '0x$1'
    ) as contract_address
  from {{ source('prices', 'trusted_tokens') }}
  where blockchain = 'sui'
),

transfers as (
  select
    t.unique_key,
    t.blockchain,
    t.block_month,
    t.block_date,
    t.block_time,
    t.checkpoint,
    t.tx_digest,
    t.token_standard,
    t.tx_from,
    t.from_resolved as "from",
    t.to_resolved as to,
    t.contract_address,
    t.coin_type,
    coalesce(m.symbol, p.symbol) as symbol,
    coalesce(m.decimals, p.decimals) as decimals,
    t.amount_raw,
    t.amount_raw / power(10, coalesce(m.decimals, p.decimals)) as amount,
    p.price as price_usd,
    t.amount_raw / power(10, coalesce(m.decimals, p.decimals)) * p.price as amount_usd,
    case when tt.contract_address is not null then true else false end as is_trusted_token,
    t.balance_delta,
    t.object_id,
    t.version,
    t.object_status,
    t.owner_type,
    t.coin_balance,
    t.prev_balance,
    t.prev_owner,
    t.has_ownership_change,
    t.transfer_type,
    t.is_supply_event,
    t.supply_event_type,
    t.transfer_direction,
    t._updated_at
  from base_transfers t
  left join coin_metadata m
    on t.coin_type_normalized = m.coin_type
  left join trusted_tokens tt
    on tt.contract_address = t.contract_address
  left join prices p
    on t.blockchain = p.blockchain
    and date_trunc('hour', t.block_time) = p.timestamp
    and t.contract_address = p.contract_address
)

select
  unique_key,
  blockchain,
  block_month,
  block_date,
  block_time,
  checkpoint,
  tx_digest,
  token_standard,
  "from",
  to,
  contract_address,
  coin_type,
  symbol,
  decimals,
  amount_raw,
  amount,
  price_usd,
  case
    when is_trusted_token = true then amount_usd
    when is_trusted_token = false and amount_usd < 1000000000 then amount_usd
    when is_trusted_token = false and amount_usd >= 1000000000 then cast(null as double)
  end as amount_usd,
  balance_delta,
  object_id,
  version,
  object_status,
  owner_type,
  coin_balance,
  prev_balance,
  prev_owner,
  has_ownership_change,
  transfer_type,
  is_supply_event,
  supply_event_type,
  transfer_direction,
  _updated_at
from transfers
