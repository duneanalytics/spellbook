{{ config(
  schema = 'sui_walrus',
  alias  = 'payments',
  materialized = 'incremental',
  file_format = 'delta',
  incremental_strategy = 'merge',
  unique_key = ['transaction_digest'],
  incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
  partition_by = ['block_month'],
  tags=['walrus']
) }}

{% set WAL_COIN = '0x2::coin::Coin<0x356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59::wal::WAL>' %}
{% set WAL_BASE = WAL_COIN | replace('0x2::coin::Coin<','') | replace('>','') %}

-- A) Walrus tx set (distinct)
with walrus_tx as (
  select distinct
      tx_register as transaction_digest_hex
    , block_date
    , block_month
  from {{ ref('sui_walrus_base_table') }}
  {% if is_incremental() %} where {{ incremental_predicate('block_date') }} {% endif %}
),

-- B) Gas from sui.transactions (base58 → hex; cast numbers to decimal(38,0))
tx_gas as (
  select
      ('0x' || lower(to_hex(from_base58(t.transaction_digest)))) as transaction_digest_hex
    , cast(t.gas_budget   as decimal(38,0))  as gas_budget_mist
    , cast(t.gas_price    as decimal(38,0))  as gas_price_mist_per_unit
    , cast(t.computation_cost as decimal(38,0)) as gas_used_computation_cost
    , cast(t.storage_cost     as decimal(38,0)) as gas_used_storage_cost
    , cast(t.storage_rebate   as decimal(38,0)) as gas_used_storage_rebate
    , cast(t.non_refundable_storage_fee as decimal(38,0)) as gas_used_non_refundable_storage_fee
    , cast(t.total_gas_cost   as decimal(38,0)) as total_gas_mist
  from {{ source('sui','transactions') }} t
  join walrus_tx w
    on ('0x' || lower(to_hex(from_base58(t.transaction_digest)))) = w.transaction_digest_hex
),

-- C) WAL deltas from events only
wal_moves as (
  select
      ('0x' || lower(to_hex(from_base58(e.transaction_digest)))) as transaction_digest_hex
    , sum(cast(json_extract_scalar(e.event_json, '$.amount') as decimal(38,0))) as wal_amount_raw
  from {{ source('sui','events') }} e
  where ('0x' || lower(to_hex(from_base58(e.transaction_digest)))) in (
    select transaction_digest_hex from walrus_tx
  )
    and (
         lower(json_extract_scalar(e.event_json, '$.coin_type')) in (lower('{{ WAL_COIN }}'), lower('{{ WAL_BASE }}'))
      or lower(json_extract_scalar(e.event_json, '$.type'))      in (lower('{{ WAL_COIN }}'), lower('{{ WAL_BASE }}'))
    )
  group by 1
),

-- D) WAL decimals (fallback to 9 if coin_info missing)
wal_meta as (
  select coalesce(max(coin_decimals), 9) as wal_decimals
  from {{ ref('dex_sui_coin_info') }}
  where lower(coin_type) in (lower('{{ WAL_COIN }}'), lower('{{ WAL_BASE }}'))
)

select
    w.transaction_digest_hex as transaction_digest
  , w.block_date
  , w.block_month
  , g.gas_budget_mist
  , g.gas_price_mist_per_unit
  , g.gas_used_computation_cost
  , g.gas_used_storage_cost
  , g.gas_used_storage_rebate
  , g.gas_used_non_refundable_storage_fee
  , g.total_gas_mist
  , m.wal_amount_raw
  , case
      when m.wal_amount_raw is not null
        then cast(cast(m.wal_amount_raw as double) / pow(10, wm.wal_decimals) as decimal(38,18))
      else null
    end as wal_amount
from walrus_tx w
left join tx_gas   g  on g.transaction_digest_hex  = w.transaction_digest_hex
left join wal_moves m  on m.transaction_digest_hex = w.transaction_digest_hex
cross join wal_meta wm
{% if is_incremental() %} 
where {{ incremental_predicate('w.block_date') }} 
{% endif %}
