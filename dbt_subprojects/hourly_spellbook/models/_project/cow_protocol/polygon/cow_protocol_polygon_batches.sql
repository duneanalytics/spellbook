{{  config(
        schema = 'cow_protocol_polygon',
        alias = 'batches',
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['tx_hash'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        post_hook='{{ expose_spells(blockchains = \'["polygon"]\',
                                    spell_type = "project",
                                    spell_name = "cow_protocol",
                                    contributors = \'["harisang"]\') }}'
    )
}}

with
solvers_ranked as (
    select
        t.block_date,
        t.block_number,
        t.tx_hash,
        t."from" as solver,
        solvers.name,
        row_number() over (partition by t.tx_hash order by t.trace_address asc nulls first) as rn
    from {{ source('polygon', 'traces') }} t
        inner join {{ ref('cow_protocol_polygon_solvers') }} solvers
            on t."from" = solvers.address
    {% if is_incremental() %}
    where {{ incremental_predicate('block_time') }}
    {% else %}
    where block_time >= timestamp '2023-08-03 16:25' --first block_time observed
    {% endif %}
),

batch_counts as (
    select try_cast(date_trunc('day', s.evt_block_time) as date) as block_date,
           s.evt_block_number,
           s.evt_block_time,
           s.evt_tx_hash,
           sr.solver,
           sr.name,
           sum(
               case
                   when selector != 0x2e1a7d4d -- unwrap
                    and selector != 0x095ea7b3 -- approval
                       then 1
                   else 0
               end
           ) as dex_swaps,
           sum(case when selector = 0x2e1a7d4d then 1 else 0 end) as unwraps,
           sum(case when selector = 0x095ea7b3 then 1 else 0 end) as token_approvals
    from {{ source('gnosis_protocol_v2_polygon', 'GPv2Settlement_evt_Settlement') }} s
        left outer join {{ source('gnosis_protocol_v2_polygon', 'GPv2Settlement_evt_Interaction') }} i
            on i.evt_tx_hash = s.evt_tx_hash
            {% if is_incremental() %}
            and {{ incremental_predicate('i.evt_block_time') }}
            {% endif %}
        left join solvers_ranked sr
            on s.evt_tx_hash = sr.tx_hash
            and s.evt_block_date = sr.block_date
            and s.evt_block_number = sr.block_number
            and sr.rn = 1
    {% if is_incremental() %}
    where {{ incremental_predicate('s.evt_block_time') }}
    {% endif %}
    group by s.evt_block_number, s.evt_block_time, s.evt_tx_hash, sr.solver, sr.name
),

batch_values as (
    select
        tx_hash,
        count(*)        as num_trades,
        sum(usd_value)  as batch_value,
        sum(fee_usd)    as fee_value,
        price           as eth_price
    from  {{ source('cow_protocol_polygon', 'trades') }}
        left outer join {{ source('prices', 'usd') }} as p
            on p.contract_address = 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270
            {% if is_incremental() %}
            and {{ incremental_predicate('minute') }}
            {% else %}
            and minute >= timestamp '2023-08-03 16:25' --first observed date for polygon
            {% endif %}
            and p.minute = date_trunc('minute', block_time)
            and blockchain = 'polygon'
    {% if is_incremental() %}
    where {{ incremental_predicate('block_time') }}
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
        inner join {{ source('polygon', 'transactions') }} tx
            on evt_tx_hash = hash
            and evt_block_number = block_number
            {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
            {% endif %}
    where num_trades > 0 --! Exclude Withdraw Batches
)

select * from combined_batch_info
