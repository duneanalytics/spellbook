{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'minipool_beacon_deposit',
    materialized = 'table'
    )
}}

with
pub_key as (
    select
        pubkey,
        minipool
    from {{ ref('rocketpool_minipool_deposit_standard') }}

    union

    select
        pubkey,
        minipool
    from {{ ref('rocketpool_minipool_deposit_credit') }}

    union

    select
        pubkey,
        minipool
    from {{ ref('rocketpool_minipool_deposit_vacant') }}
)

select
    pub_key.minipool,
    pub_key.pubkey,
    sum(
        bytearray_to_uint256(bytearray_reverse(dep.amount)) / 1e9
    ) as beacon_amount_deposited
from
    {{ source('eth2_ethereum','DepositContract_evt_DepositEvent') }} as dep
right join pub_key
    on pub_key.pubkey = dep.pubkey
group by
    1,
    2