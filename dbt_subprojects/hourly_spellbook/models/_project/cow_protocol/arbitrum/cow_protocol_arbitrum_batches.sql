{{  config(
        schema = 'cow_protocol_arbitrum',
        alias = 'batches',
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['tx_hash'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        post_hook='{{ expose_spells(blockchains = \'["arbitrum"]\',
                                    spell_type = "project",
                                    spell_name = "cow_protocol",
                                    contributors = \'["olgafetisova"]\') }}'
    )
}}

WITH
batch_counts as (
    select try_cast(date_trunc('day', s.evt_block_time) as date) as block_date,
           s.evt_block_number,
           s.evt_block_time,
           s.evt_tx_hash,
           solver,
           name,
           sum(
               case
                   when selector != 0x2e1a7d4d -- unwrap
                    and selector != 0x095ea7b3 -- approval
                       then 1
                   else 0
                end)                                                as dex_swaps,
           sum(case when selector = 0x2e1a7d4d then 1 else 0 end) as unwraps,
           sum(case when selector = 0x095ea7b3 then 1 else 0 end) as token_approvals
    from {{ source('gnosis_protocol_v2_arbitrum', 'GPv2Settlement_evt_Settlement') }} s
        left outer join {{ source('gnosis_protocol_v2_arbitrum', 'GPv2Settlement_evt_Interaction') }} i
            on i.evt_tx_hash = s.evt_tx_hash
            {% if is_incremental() %}
            AND {{ incremental_predicate('i.evt_block_time') }}
            {% endif %}
        join join {{ source('cow_protocol_arbitrum', 'cow_protocol_arbitrum_solvers') }}
            on solver = address
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('s.evt_block_time') }}
    {% endif %}
    group by s.evt_block_number, s.evt_block_time, s.evt_tx_hash, solver, name
),

batch_values as (
    select
        tx_hash,
        count(*)        as num_trades,
        sum(usd_value)  as batch_value,
        sum(fee_usd)    as fee_value,
        price           as eth_price
    from  {{ source('cow_protocol_arbitrum', 'trades') }}
        left outer join {{ source('prices', 'usd') }} as p
            on p.contract_address = 0x82af49447d8a07e3bd95bd0d56f35241523fbab1
            {% if is_incremental() %}
            and {{ incremental_predicate('minute') }}
            {% endif %}
            and p.minute = date_trunc('minute', block_time)
            and blockchain = 'arbitrum'
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
    group by tx_hash, price
),

combined_batch_info as (
    select
        b.block_date,
        evt_block_number                               as block_number,
        evt_block_time                                 as block_time,
        num_trades,
        dex_swaps,
        batch_value,
        solver                                           as solver_address,
        evt_tx_hash                                      as tx_hash,
        gas_price,
        gas_used,
        ((gas_price / pow(10, 9)) * gas_used * eth_price) / pow(10, 9) as tx_cost_usd,
        fee_value,
        2 * bytearray_length(data) / 1024                     as call_data_size,
        unwraps,
        token_approvals
    from batch_counts b
        join batch_values t
            on b.evt_tx_hash = t.tx_hash
        inner join {{ source('arbitrum', 'transactions') }} tx
            on evt_tx_hash = hash
            {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
            {% endif %}
    where num_trades > 0 --! Exclude Withdraw Batches
)

select * from combined_batch_info
