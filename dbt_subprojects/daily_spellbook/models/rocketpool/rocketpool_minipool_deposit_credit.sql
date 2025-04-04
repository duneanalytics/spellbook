{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'minipool_deposit_credit',
    materialized = 'table',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['minipool','call_block_time']
)
}}

with
deposit_with_credit_calls as (
    select
        _expectedMinipoolAddress as minipool,
        call_block_time,
        call_tx_hash as tx_hash,
        coalesce(_bondAmount, cast('16000000000000000000' as uint256)) as bond_amount, 
        _validatorPubkey as pubkey,
        _minimumNodeFee / 1e18 as node_fee
    from
        {{ source('rocketpool_ethereum','RocketNodeDeposit_call_depositWithCredit') }}
    where
        call_success = true
),

transaction_values as (
    select
        hash,
        value
    from
        {{ source('ethereum','transactions') }}
    where
        block_time > cast('2023-04-16' as timestamp)
)

select
    dep.minipool,
    dep.call_block_time,
    dep.bond_amount / 1e18 as bond_amount,
    dep.pubkey,
    dep.node_fee
from
    deposit_with_credit_calls as dep
inner join transaction_values as trans on dep.tx_hash = trans.hash
where trans.value <= dep.bond_amount
