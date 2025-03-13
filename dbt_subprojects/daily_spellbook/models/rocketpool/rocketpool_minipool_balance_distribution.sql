{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'minipool_balance_distribution',
    materialized = 'table',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['minipool'],
    )
}}

select
    trans.to as minipool,
    true as is_distributed
from
    {{ source('rocketpool_ethereum','RocketMinipoolDelegate_call_distributeBalance') }} as dist
inner join {{ source('ethereum','transactions') }} as trans 
    on dist.call_tx_hash = trans.hash
where
    dist.call_block_time > timestamp '2023-04-01'
    and trans.block_time > timestamp '2023-04-01'
    and dist._rewardsOnly = false -- Only full minipool distributions
    and dist.call_success = true