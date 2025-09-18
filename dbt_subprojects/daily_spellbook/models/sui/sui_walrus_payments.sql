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

-- A) Walrus tx set (one row per tx_register)
with walrus_tx as (
  select
      transaction_digest_hex
      , block_date
      , block_month
  from (
    select
        bt.tx_register  as transaction_digest_hex
        , bt.block_date   as block_date
        , bt.block_month  as block_month
        , row_number() over (
          partition by bt.tx_register
          order by bt.ts_register asc, bt.evt_index_register asc
        ) as rn
    from {{ ref('sui_walrus_base_table') }} bt
    {% if is_incremental() %}
      where {{ incremental_predicate('bt.block_date') }}
    {% endif %}
  ) s
  where rn = 1
)

-- B) Gas (1 row per transaction)
, tx_gas as (
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
from walrus_tx w
left join tx_gas g
  on g.transaction_digest_hex = w.transaction_digest_hex
{% if is_incremental() %}
where {{ incremental_predicate('w.block_date') }}
{% endif %}