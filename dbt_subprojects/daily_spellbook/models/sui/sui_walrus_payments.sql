{{ config(
    schema = 'sui_walrus',
    alias  = 'payments',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest','block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    partition_by = ['block_month'],
    tags=['walrus']
) }}

-- A) Walrus tx set (one row per tx_register)
with walrus_tx as (
  select
      bt.tx_register
      , bt.block_date
      , bt.block_month
      , row_number() over (
        partition by bt.tx_register
        order by bt.ts_register asc, bt.evt_index_register asc
      ) as rn
  from {{ ref('sui_walrus_base_table') }} bt
  {% if is_incremental() %}
    where {{ incremental_predicate('bt.block_date') }}
  {% endif %}
),
walrus_tx_dedup as (
  select 
      tx_register as transaction_digest 
      , block_date
      , block_month
  from walrus_tx
  where rn = 1
)

-- B) Gas
, tx_gas AS (
  SELECT
      t.transaction_digest
      , CAST(t.gas_budget AS DECIMAL(38,0))         AS gas_budget_mist
      , CAST(t.gas_price AS DECIMAL(38,0))          AS gas_price_mist_per_unit
      , CAST(t.computation_cost AS DECIMAL(38,0))   AS gas_used_computation_cost
      , CAST(t.storage_cost AS DECIMAL(38,0))       AS gas_used_storage_cost
      , CAST(t.storage_rebate AS DECIMAL(38,0))     AS gas_used_storage_rebate
      , CAST(t.non_refundable_storage_fee AS DECIMAL(38,0)) AS gas_used_non_refundable_storage_fee
      , CAST(t.total_gas_cost AS DECIMAL(38,0))     AS total_gas_mist
  FROM {{ source('sui','transactions') }} t
  INNER JOIN walrus_tx_dedup w
    ON t.transaction_digest = w.transaction_digest
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('w.block_date') }}
  {% endif %}
)

select
    w.transaction_digest                                       as transaction_digest
    , w.block_date
    , w.block_month
    , g.gas_budget_mist
    , g.gas_price_mist_per_unit
    , g.gas_used_computation_cost
    , g.gas_used_storage_cost
    , g.gas_used_storage_rebate
    , g.gas_used_non_refundable_storage_fee
    , g.total_gas_mist
from walrus_tx_dedup w
left join tx_gas g
  on g.transaction_digest = w.transaction_digest
{% if is_incremental() %}
where {{ incremental_predicate('w.block_date') }}
{% endif %}
