CREATE OR REPLACE VIEW balancer.view_pools_tokens_weights AS
with events as (
    -- binds
    select call_block_number as block_number, index, call_trace_address,
    contract_address as pool, token, denorm
    from balancer."BPool_call_bind"
    INNER JOIN ethereum.transactions ON call_tx_hash = hash
    where call_success

    union all

    -- rebinds
    select call_block_number as block_number, index, call_trace_address, 
    contract_address as pool, token, denorm
    from balancer."BPool_call_rebind"
    INNER JOIN ethereum.transactions ON call_tx_hash = hash
    where call_success

    union all
    
    -- unbinds
    select call_block_number as block_number,  index, call_trace_address,
    contract_address as pool, token, 0 as denorm
    from balancer."BPool_call_unbind"
    INNER JOIN ethereum.transactions ON call_tx_hash = hash
    where call_success
),
state_with_gaps as (
    select events.block_number, events.pool, events.token, events.denorm,
    LEAD(cast(events.block_number as text), '1', '99999999') over (
        partition by events.pool, events.token 
        order by events.block_number, index, call_trace_address) as next_block_number
    from events 
), 
settings as (
    select pool, 
    token, 
    denorm
    from state_with_gaps s
    where next_block_number = '99999999'
    and denorm > 0
),
sum_denorm as (
    select pool, sum(denorm) as sum_denorm
    from state_with_gaps s
    where next_block_number = '99999999'
    and denorm > 0
    group by pool
),
norm_weights as (
    select settings.pool AS pool_address, token AS token_address, denorm/sum_denorm as normalized_weight
    from settings inner join sum_denorm on settings.pool = sum_denorm.pool
)
select pool_address as pool_id, token_address,  normalized_weight
from norm_weights

union all

select c."poolId" as pool_id, unnest(cc.tokens) as token_address, unnest(cc.weights)/1e18 as normalized_weight
from balancer_v2."Vault_evt_PoolRegistered" c
inner join balancer_v2."WeightedPoolFactory_call_create" cc
on c.evt_tx_hash = cc.call_tx_hash

union all

select c."poolId" as pool_id, unnest(cc.tokens) as token_address, unnest(cc.weights)/1e18 as normalized_weight
from balancer_v2."Vault_evt_PoolRegistered" c
inner join balancer_v2."WeightedPool2TokensFactory_call_create" cc
on c.evt_tx_hash = cc.call_tx_hash
