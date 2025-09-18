{{ config(
    schema = 'sui_walrus'
  , alias  = 'payments'
  , materialized = 'incremental'
  , file_format = 'delta'
  , incremental_strategy = 'merge'
  , unique_key = ['transaction_digest']
  , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
  , partition_by = ['block_month']
  , tags=['walrus']
) }}

-- A) Walrus tx set (distinct)
with walrus_tx as (
  select distinct
      tx_register                              as transaction_digest_hex
    , block_date
    , block_month
  from {{ ref('sui_walrus_base_table') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('block_date') }}
  {% endif %}
)

-- B) Gas from sui.transactions (base58 → hex; cast numbers to decimal(38,0))
, tx_gas as (
  select
      ('0x' || lower(to_hex(from_base58(t.transaction_digest)))) as transaction_digest_hex
    , cast(t.gas_budget   as decimal(38,0))  as gas_budget_mist              -- varchar → decimal
    , cast(t.gas_price    as decimal(38,0))  as gas_price_mist_per_unit      -- decimal(20,0) → decimal(38,0)
    , cast(t.computation_cost as decimal(38,0)) as gas_used_computation_cost -- bigint → decimal(38,0)
    , cast(t.storage_cost     as decimal(38,0)) as gas_used_storage_cost
    , cast(t.storage_rebate   as decimal(38,0)) as gas_used_storage_rebate
    , cast(t.non_refundable_storage_fee as decimal(38,0)) as gas_used_non_refundable_storage_fee
    , cast(t.total_gas_cost   as decimal(38,0)) as total_gas_mist
  from {{ source('sui','transactions') }} t
  join walrus_tx w
    on ('0x' || lower(to_hex(from_base58(t.transaction_digest)))) = w.transaction_digest_hex
)

select
    w.transaction_digest_hex as transaction_digest
  , bt.block_date
  , bt.block_month

  -- gas
  , g.gas_budget_mist
  , g.gas_price_mist_per_unit
  , g.gas_used_computation_cost
  , g.gas_used_storage_cost
  , g.gas_used_storage_rebate
  , g.gas_used_non_refundable_storage_fee
  , g.total_gas_mist
from walrus_tx w
join {{ ref('sui_walrus_base_table') }} bt
  on bt.tx_register = w.transaction_digest_hex
left join tx_gas g
  on g.transaction_digest_hex = w.transaction_digest_hex
{% if is_incremental() %}
where {{ incremental_predicate('bt.block_date') }}
{% endif %}
