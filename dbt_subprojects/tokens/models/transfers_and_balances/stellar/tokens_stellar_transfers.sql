{{
  config(
    schema = 'tokens_stellar',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_date'],
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    merge_skip_unchanged = true,
    post_hook = '{{ hide_spells() }}',
  )
}}

{% set stellar_transfer_start_date = '2026-01-01' %} -- ci test, revert to '2015-09-30'
{% set xlm_decimals = 7 %}
{% set xlm_contract_id = 'CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA' %}
{% set xlm_native_price_address_varchar = '0x0000000000000000000000000000000000000000' %}

with

-- source data contains dupes on all columns except updated_at and ingested_at
-- until (if?) it gets resolved - applying group by to get the 1st updated_at
base_transfers as (
  select
    t.unique_key,
    t.transaction_hash,
    t.transaction_id,
    t.operation_id,
    cast(t.closed_at as date) as block_date,
    date_trunc('month', t.closed_at) as block_month,
    t.closed_at as block_time,
    t.ledger_sequence,
    t."from",
    t.to,
    t.to_muxed,
    t.to_muxed_id,
    t.contract_id as contract_address,
    t.asset,
    t.asset_type,
    t.asset_code,
    t.asset_issuer,
    t.amount_raw,
    t.event_topic,
    t.event_type,
    t.is_soroban,
    min(t.updated_at) as _updated_at
  from {{ source('stellar', 'token_transfers') }} t
  where t.closed_at >= timestamp '{{ stellar_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('t.closed_at') }}
    {% endif %}
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
),

prices as (
  select
    p.timestamp,
    case
      when p.contract_address_varchar = '{{ xlm_native_price_address_varchar }}'
        then '{{ xlm_contract_id }}'
      else p.contract_address_varchar
    end as contract_address,
    p.symbol,
    p.decimals,
    p.price
  from {{ source('prices_external', 'hour') }} p
  where p.blockchain = 'stellar'
    and p.timestamp >= timestamp '{{ stellar_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('p.timestamp') }}
    {% endif %}
)

select
  b.unique_key,
  'stellar' as blockchain,
  b.block_month,
  b.block_date,
  b.block_time,
  b.ledger_sequence,
  b.transaction_hash,
  b.transaction_id,
  b.operation_id,
  case
    when b.asset_type = 'native' then 'native'
    when b.asset_type like 'credit_alphanum%' then 'classic'
    else 'soroban'
  end as token_standard,
  b."from",
  b.to,
  b.to_muxed,
  b.to_muxed_id,
  b.contract_address,
  b.asset,
  b.asset_type,
  b.asset_code,
  b.asset_issuer,
  coalesce(case when b.asset_type = 'native' then 'XLM' else b.asset_code end, p.symbol) as symbol,
  coalesce(p.decimals, {{ xlm_decimals }}) as decimals,
  try_cast(b.amount_raw as double) as amount_raw,
  try_cast(b.amount_raw as double) / power(10, coalesce(p.decimals, {{ xlm_decimals }})) as amount,
  p.price as price_usd,
  try_cast(b.amount_raw as double) / power(10, coalesce(p.decimals, {{ xlm_decimals }})) * p.price as amount_usd,
  b.event_topic,
  b.event_type,
  b.is_soroban,
  b._updated_at
from base_transfers b
left join prices p
  on date_trunc('hour', b.block_time) = p.timestamp
  and b.contract_address = p.contract_address
