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

with base as (
  select
      from_unixtime(timestamp_ms/1000)                          as block_time
    , date(from_unixtime(timestamp_ms/1000))                    as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000))     as block_month
    , ('0x' || lower(to_hex(transaction_digest))) as transaction_digest
    , ('0x' || lower(to_hex(sender))) as sender
    , execution_success
    , is_sponsored_tx
    , has_zklogin_sig
    , transaction_count

    -- gas (Mist)
    , total_gas_cost
    , computation_cost
    , storage_cost
    , storage_rebate
    , non_refundable_storage_fee
  from {{ source('sui','transactions') }}
  where transaction_kind = 'ProgrammableTransaction'
    and is_system_txn = false
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ sui_project_start_date }}'
),

filtered as (
  select *
  from base
  {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
  {% endif %}
),

daily as (
  select
      block_date
    , max(block_month) as block_month

    -- sanity: rows vs distinct PTBs
    , count(*)                                                   as rows_cnt
    , count(distinct transaction_digest)                         as ptb_cnt
    , (count(*) = count(distinct transaction_digest))            as rows_vs_ptbs_is_one_to_one

    -- distinct senders (headline)
    , count(distinct sender)                                     as senders

    -- commands from successful PTBs
    , coalesce(sum(case when execution_success then transaction_count else 0 end), 0)
        as commands_from_successful_ptbs

    -- success counts & share of PTBs
    , count(distinct case when execution_success then transaction_digest end)
        as successful_ptbs_n
    , case when count(distinct transaction_digest) > 0
           then cast(count(distinct case when execution_success then transaction_digest end) as double)
                / cast(count(distinct transaction_digest) as double)
           else null end
        as successful_ptbs_pct

    -- sponsored among successful PTBs
    , count(distinct case when execution_success and is_sponsored_tx then transaction_digest end)
        as successful_ptbs_sponsored_n
    , case when count(distinct case when execution_success then transaction_digest end) > 0
           then cast(count(distinct case when execution_success and is_sponsored_tx then transaction_digest end) as double)
                / cast(count(distinct case when execution_success then transaction_digest end) as double)
           else null end
        as successful_ptbs_sponsored_pct

    -- zkLogin among successful PTBs
    , count(distinct case when execution_success and has_zklogin_sig then transaction_digest end)
        as successful_ptbs_zklogin_n
    , case when count(distinct case when execution_success then transaction_digest end) > 0
           then cast(count(distinct case when execution_success and has_zklogin_sig then transaction_digest end) as double)
                / cast(count(distinct case when execution_success then transaction_digest end) as double)
           else null end
        as successful_ptbs_zklogin_pct

    -- Gas totals (Mist)
    , coalesce(sum(total_gas_cost), 0)               as total_gas_cost_mist
    , coalesce(sum(computation_cost), 0)             as computation_cost_mist
    , coalesce(sum(storage_cost), 0)                 as storage_cost_incurred_mist
    , coalesce(sum(storage_rebate), 0)               as storage_rebated_mist
    , coalesce(sum(non_refundable_storage_fee), 0)   as non_refundable_storage_fee_mist

    -- Gas totals in SUI
    , cast(sum(total_gas_cost)              as double) / 1e9 as gas_fees_sui
    , cast(sum(computation_cost)            as double) / 1e9 as computation_cost_sui
    , cast(sum(storage_cost)               as double) / 1e9 as storage_cost_incurred_sui
    , cast(sum(storage_rebate)             as double) / 1e9 as storage_rebated_sui
    , cast(sum(non_refundable_storage_fee) as double) / 1e9 as non_refundable_storage_fee_sui
  from filtered
  group by 1
)

select *
from daily