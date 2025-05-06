{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'minipool_beacon_withdrawal',
    materialized = 'table'
)
}}

with
pub_key as (
    select
        minipool,
        pubkey,
        validator_index
    from
        {{ ref('rocketpool_minipool_pubkey_index')}}
),

withdrawals as (
    select
        wth.block_time as t,
        pky.validator_index,
        wth.amount / 1e9 as amount,
        pky.minipool,
        pky.pubkey
    from {{ source('ethereum','withdrawals') }} as wth
    right join pub_key as pky
        on pky.validator_index = wth.validator_index
)

select
    minipool,
    validator_index,
    pubkey,
    max(t) as last_withdrawal_t,
    sum(amount) as beacon_amount_withdrawn,
    sum(if(amount < 8, amount, 0)) as beacon_amount_skim_withdrawn,
    bool_or(amount > 8) as exited
from
    withdrawals
group by
    1,
    2,
    3
