{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'minipool_balance_distribution',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = 'minipool'
    )
}}

select distinct
    trans.to as minipool,
    true as is_distributed
from
    {{ source('ethereum','transactions') }} as trans 
right join {{ source('rocketpool_ethereum','RocketMinipoolDelegate_call_distributeBalance') }} as dist
    on dist.call_tx_hash = trans.hash
        and dist.call_block_number = trans.block_number
        and dist._rewardsOnly = false -- Only full minipool distributions
        and dist.call_success = true
        {% if is_incremental() -%}
        and {{ incremental_predicate('dist.call_block_time') }}
        {% else -%}
        and dist.call_block_time > timestamp '2023-04-01'
        {% endif -%}
where
    dist.call_block_time > timestamp '2023-04-01'
    and trans.block_time > timestamp '2023-04-01'
    and dist._rewardsOnly = false -- Only full minipool distributions
    and dist.call_success = true