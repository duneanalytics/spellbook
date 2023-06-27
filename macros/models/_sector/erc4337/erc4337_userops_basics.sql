{% macro erc4337_userops_basics(
    blockchain='',
    version='',
    userops_evt_model=null,
    handleops_call_model=null
    )
%}

with userop as (
    select
          '{{ blockchain }}' as blockchain
        , '{{ version }}' as version
        , evt_block_time as block_time
        , contract_address as entrypoint_contract
        , evt_tx_hash as tx_hash
        , sender
        , userOpHash as userop_hash
        , success
        , paymaster
        , actualGasCost/1e18 as op_fee
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
    , userop.op_fee
    , handleops.beneficiary
from userop
left join handleops on userop.tx_hash = handleops.tx_hash

{% endmacro %}
