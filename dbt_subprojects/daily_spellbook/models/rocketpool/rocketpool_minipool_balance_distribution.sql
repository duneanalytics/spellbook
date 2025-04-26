{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'minipool_balance_distribution',
    materialized = 'table'
    )
}}

select
    trans.to as minipool,
    true as is_distributed
from
    {{ source('ethereum','transactions') }} as trans 
right join {{ source('rocketpool_ethereum','RocketMinipoolDelegate_call_distributeBalance') }} as dist
    on dist.call_tx_hash = trans.hash
        and dist.call_block_number = trans.block_number
where
    dist.call_block_time > timestamp '2023-04-01'
    and trans.block_time > timestamp '2023-04-01'
    and dist._rewardsOnly = false -- Only full minipool distributions
    and dist.call_success = true