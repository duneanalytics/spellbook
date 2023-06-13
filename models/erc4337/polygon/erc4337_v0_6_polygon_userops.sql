{{ config
(
    schema = 'erc4337_v0_6_polygon',
    alias ='userops',
    partition_by = ['block_time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly"]\') }}'
)
}}


{% set chain = 'polygon' %}
{% set gas_symbol = 'MATIC' %}
{% set wrapped_gas_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270' %}
{% set version = 'v0.6' %}
{% set deployed_date = '2023-02-15' %}


with userop as (
    select 
          '{{chain}}' as blockchain
        , '{{version}}' as version
        , evt_block_time as block_time
        , contract_address as entrypoint_contract
        , evt_tx_hash as tx_hash
        , sender
        , userOpHash as userop_hash
        , success
        , paymaster
        , actualGasCost as op_gas_cost
        , actualGasUsed as op_gas_used
    from {{ source('erc4337_polygon','EntryPoint_v0_6_evt_UserOperationEvent') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
, handleops as (
    select call_tx_hash as tx_hash, beneficiary from {{source('erc4337_polygon', 'EntryPoint_v0_6_call_handleOps')}}
    {% if is_incremental() %}
        WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
, txs as (
    select 
          hash as tx_hash
        , "from" as tx_from
        , to as tx_to
        , '{{gas_symbol}}' as gas_symbol
        , cast(gas_price as bigint) as tx_gas_price
        , gas_used as tx_gas_used
        , cast(gas_price as bigint)*gas_used as tx_gas_cost
    from polygon.transactions 
    where hash in (
        select tx_hash from userop
    )
    and block_time > timestamp  '{{deployed_date}}'
    {% if is_incremental() %}
        and block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
, price as (
    select symbol, decimals, minute, price  
    from {{source('prices','usd')}}
    where minute > timestamp  '{{deployed_date}}'
        and contract_address={{wrapped_gas_address}}
        and blockchain='{{chain}}'
    {% if is_incremental() %}
        and block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
select 
      userop.blockchain
    , userop.version
    , userop.block_time
    , userop.entrypoint_contract
    , userop.tx_hash
    , userop.sender
    , userop.userop_hash
    , userop.success
    , userop.paymaster
    , userop.op_gas_cost/1e18 as op_gas_cost
    , userop.op_gas_cost*price.price/1e18 as op_gas_cost_usd
    , txs.tx_from as bundler
    , txs.tx_to
    , txs.gas_symbol
    , cast(txs.tx_gas_cost as double)/1e18 as tx_gas_cost
    , cast(txs.tx_gas_cost as double)*price.price/1e18 as tx_gas_cost_usd
    , handleops.beneficiary
from userop 
left join txs on userop.tx_hash = txs.tx_hash
left join handleops on userop.tx_hash = handleops.tx_hash
left join price on date_trunc('minute', userop.block_time) = price.minute