{{
    config(
        schema='pyth_entropy_abstract',
        alias='request',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash', 'assigned_sequence_number']
    )
}}

{{pyth_entropy_request(
    blockchain='abstract',
    symbol='ETH',
    entropy_address='0x5a4a369F4db5df2054994AF031b7b23949b98c0e'
)}}
and slice(trace_address, -1, 1) = 0
-- Zksync does a weird thing where native ether need to be handled by system contract
-- before it called the contract again??? this filter out to be the second call
-- https://docs.zksync.io/zksync-protocol/differences/evm-instructions#call-staticcall-delegatecall