{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'minipool_deposit_vacant',
    materialized = 'table'
)
}}

with deposits as (
    select
        _expectedminipooladdress as minipool,
        call_block_time,
        _bondamount / 1e18 as bond_amount,
        _validatorpubkey as pubkey,
        _minimumnodefee as node_fee
    from
        {{ source('rocketpool_ethereum','rocketnodedeposit_call_createvacantminipool') }}
    where call_success = true
)
,
/* there were duplicate public keys used on 5 vacant minipools.  this will ensure minipool is valid */
promoted as (
    select to as minipool
    from
        {{ source('ethereum','transactions') }}
    where
        data = 0x13dc01dc /*promote*/
        and to in (
            select minipool
            from
                deposits
        )
        and success = true
        and block_time > cast('2023-04-17' as timestamp)
)

select
    deposits.minipool,
    deposits.call_block_time,
    deposits.bond_amount,
    deposits.pubkey,
    deposits.node_fee
from
    deposits
inner join promoted on
    deposits.minipool = promoted.minipool
