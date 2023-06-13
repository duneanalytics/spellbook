{% macro erc4337_userops_trades(
    blockchain='',
    models=[],
    userops_evt_model=null,
    handleops_call_model=null,
    transactions_model=null,
    tokens_erc20_model=null,
    prices_model=null
    )
%}

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
        , actualGasCost as op_fee
        , actualGasUsed as op_gas_used
    from {{ userops_evt_model }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
, handleops as (
    select call_tx_hash as tx_hash,
           beneficiary
    from {{ handleops_call_model }}
    {% if is_incremental() %}
        WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
, txs as (
    select
          hash as tx_hash
        , tx.from as tx_from
        , tx.to as tx_to
        , '{{gas_symbol}}' as gas_symbol
        , cast(gas_price as bigint) as tx_gas_price
        , gas_used as tx_gas_used
        , effective_gas_price * gas_used as tx_fee
    from {{ transactions_model }} tx
    where hash in (
        select tx_hash from userop
    )
    and block_time > timestamp '{{deployed_date}}'
    {% if is_incremental() %}
        and block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
, price as (
    select symbol, decimals, minute, price
    from {{ prices_model }}
    where minute > timestamp  '{{deployed_date}}'
        and contract_address='{{wrapped_gas_address}}'
        and blockchain='{{chain}}'
    {% if is_incremental() %}
        and minute >= date_trunc("day", now() - interval '1 week')
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
    , userop.op_fee/1e18 as op_fee
    , userop.op_fee*price.price/1e18 as op_fee_usd
    , txs.tx_from as bundler
    , txs.tx_to
    , txs.gas_symbol
    , cast(txs.tx_fee as double)/1e18 as tx_fee
    , cast(txs.tx_fee as double)*price.price/1e18 as tx_fee_usd
    , handleops.beneficiary
from userop
left join txs on userop.tx_hash = txs.tx_hash
left join handleops on userop.tx_hash = handleops.tx_hash
left join price on date_trunc('minute', userop.block_time) = price.minute