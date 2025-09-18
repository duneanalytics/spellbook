{{ config(
    schema = 'sui_daily',
    alias = 'stats',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date'],
    incremental_predicates = [ incremental_predicate('DBT_INTERNAL_DEST.block_date') ]
) }}

{% set sui_project_start_date = var('sui_project_start_date', '2025-09-01') %}

-- Pull only what we need; keep native datatypes (sender varbinary, digest varchar)
with src as (
  select
      date                                                       as block_date              -- native DATE
    , date_trunc('month', date)                                  as block_month             -- needed for partition
    , transaction_digest                                         as transaction_digest      -- varchar (base58)
    , sender                                                     as sender_vb               -- varbinary
    , execution_success                                          as execution_success       -- boolean
    , is_sponsored_tx                                            as is_sponsored_tx         -- boolean
    , has_zklogin_sig                                            as has_zklogin_sig         -- boolean
    , transaction_count                                          as transaction_count_d20   -- decimal(20,0)
    , total_gas_cost                                             as total_gas_cost_d20      -- decimal(20,0)
    , computation_cost                                           as computation_cost_d20    -- decimal(20,0)
    , storage_cost                                               as storage_cost_d20        -- decimal(20,0)
    , storage_rebate                                             as storage_rebate_d20      -- decimal(20,0)
    , non_refundable_storage_fee                                 as nrs_fee_d20             -- decimal(20,0)
  from {{ source('sui','transactions') }}
  where transaction_kind = 'ProgrammableTransaction'
    and is_system_txn = false
    and date >= date '{{ sui_project_start_date }}'
),

filtered as (
  select *
  from src
  {% if is_incremental() %}
    where {{ incremental_predicate('src.block_date') }}
  {% endif %}
),

-- Single aggregation pass; keep names aligned to the “ask”
daily as (
  select
      block_date                                         as block_date
    , max(block_month)                                   as block_month

    -- data quality
    , count(*)                                           as rows_checkon_ptbs
    , count(distinct transaction_digest)                 as ptbs

    -- distinct senders
    , count(distinct sender_vb)                          as senders

    -- commands from successful PTBs
    , coalesce(
        sum(case when execution_success then cast(transaction_count_d20 as bigint) else 0 end), 0
      )                                                  as commands_successful_ptbs

    -- success counts & pct (over all PTBs)
    , count(distinct case when execution_success then transaction_digest end)
                                                        as ptbs_success
    , case when count(distinct transaction_digest) > 0
           then cast(count(distinct case when execution_success then transaction_digest end) as double)
                / cast(count(distinct transaction_digest) as double)
           else null end                                 as pct_ptbs_success

    -- sponsored among successful PTBs
    , count(distinct case when execution_success and is_sponsored_tx then transaction_digest end)
                                                        as ptbs_success_sponsored
    , case when count(distinct case when execution_success then transaction_digest end) > 0
           then cast(count(distinct case when execution_success and is_sponsored_tx then transaction_digest end) as double)
                / cast(count(distinct case when execution_success then transaction_digest end) as double)
           else null end                                 as pct_ptbs_success_sponsored

    -- zkLogin among successful PTBs
    , count(distinct case when execution_success and has_zklogin_sig then transaction_digest end)
                                                        as ptbs_success_zklogin
    , case when count(distinct case when execution_success then transaction_digest end) > 0
           then cast(count(distinct case when execution_success and has_zklogin_sig then transaction_digest end) as double)
                / cast(count(distinct case when execution_success then transaction_digest end) as double)
           else null end                                 as pct_ptbs_success_zklogin

    -- Gas totals in SUI (divide Mist by 1e9)
    , cast(sum(total_gas_cost_d20)           as double) / 1e9  as total_gas_cost
    , cast(sum(computation_cost_d20)         as double) / 1e9  as computation_cost
    , cast(sum(storage_cost_d20)             as double) / 1e9  as storage_cost
    , cast(sum(storage_rebate_d20)           as double) / 1e9  as storage_rebate
    , cast(sum(nrs_fee_d20)                  as double) / 1e9  as non_refundable_storage_fee
  from filtered
  group by 1
)

select
    block_date
  , block_month
  , senders
  , ptbs
  , rows_checkon_ptbs
  , commands_successful_ptbs
  , ptbs_success
  , pct_ptbs_success
  , ptbs_success_sponsored
  , pct_ptbs_success_sponsored
  , ptbs_success_zklogin
  , pct_ptbs_success_zklogin
  , total_gas_cost
  , computation_cost
  , storage_cost
  , storage_rebate
  , non_refundable_storage_fee
from daily