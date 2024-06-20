{{ config(
    schema = 'pyth_celo',
    alias = 'transactions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(\'["celo"]\',
                            "project",
                            "pyth",
                            \'["synthquest"]\') }}'
        )
}}


{% set project_start_date = '2022-12-06' %}

with deploy_base as (
    select 
          chain
        , cast(contract_address as varbinary) as contract_address
        , chain_type
    FROM
        {{ ref('pyth_price_feed_contracts') }}
    where 
        chain_type = 'evm'
)

, pyth_chain_contract as (
    select chain, contract_address, chain_type from deploy_base
    where chain = 'celo'
)

select 
      pcc.chain
    , pcc.contract_address as pyth_contract
    , pcc.chain_type
    , tr.block_time
    , date_trunc('day', tr.block_time) as block_date
    , tr.block_number
    , tr.value as trace_value
    , txs.value as txn_value
    , tr.gas_used as trace_gas_used
    , txs.gas_used as txn_gas_used
    , tr.tx_hash
    , tr."from" as trace_from
    , tr.to as trace_to
    , txs."from" as txn_from
    , txs.to as txn_to
    , tr.call_type
    , array_join(tr.trace_address, ',') as trace_address
    , tr.sub_traces
    , ((tr.gas_used * POWER(10, -9)) * (txs.gas_price * POWER(10, -9))) AS trace_gas_paid_in_eth
    -- L2 fee only
    , ((txs.gas_used * POWER(10, -9)) * (txs.gas_price * POWER(10, -9))) AS txn_gas_paid_in_eth
    , bytearray_substring(tr.input, 1, 4) as function_signature
    -- manually change namespace for unlabeled or mislabeled contracts
    , case when tr."from" = 0x3fc194fa6c26be94fc404e69b51793c199c3bf52  or tr."from" = 0x3d07cbc5cb9376a67e76c0655fe239dda8e2b264 then 'synthetix_v3' else bc.namespace 
        end as "namespace"
    , bc.name
    
from {{ source('celo', 'traces') }} tr
inner join pyth_chain_contract pcc on tr.to = pcc.contract_address and pcc.chain = 'celo'
inner join {{ source('celo', 'transactions') }} txs on tr.tx_hash = txs.hash
left join {{ source('celo', 'contracts') }} bc on tr."from" = bc.address
where 
tr.tx_success = true

{% if is_incremental() %}
and {{ incremental_predicate('tr.block_time') }}
and {{ incremental_predicate('txs.block_time') }}
{% else %}
and tr.block_time >= DATE '{{project_start_date}}'
and txs.block_time >= DATE '{{project_start_date}}'
{% endif %}

order by tr.block_time desc
