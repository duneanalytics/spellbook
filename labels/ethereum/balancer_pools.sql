-- TODO: improve for cases where there are multiple binds/rebinds/unbinds in the same block
with events as (
    -- binds
    select call_block_number as block_number, 
    contract_address as pool, token, denorm
    from balancer."BPool_call_bind"
    where call_success

    union all

    -- rebinds
    select call_block_number as block_number, 
    contract_address as pool, token, denorm
    from balancer."BPool_call_rebind"
    where call_success

    union all
    
    -- unbinds
    select call_block_number as block_number, 
    contract_address as pool, token, 0 as denorm
    from balancer."BPool_call_unbind"
    where call_success
),
state_with_gaps as (
    select events.block_number, events.pool, events.token, events.denorm,
    LEAD(cast(events.block_number as text), '1', '99999999') over (partition by events.pool, events.token order by events.block_number) as next_block_number
    from events 
), 
settings as (
    select pool, 
    coalesce(t.symbol,'?') as symbol, 
    denorm
    from state_with_gaps s
    left join erc20.tokens t on s.token = t.contract_address
    where next_block_number = '99999999'
    and denorm > 0
),
final as (
    SELECT 
      pool as address, 
      lower(CONCAT(string_agg(symbol, '/'), ' ', string_agg(cast(norm_weight as text), '/'))) AS label,
      'balancer_pool' AS type,
      'balancerlabs' as author

    FROM   (
        select s1.pool, symbol, cast(100*denorm/total_denorm as integer) as norm_weight from settings s1
        inner join (select pool, sum(denorm) as total_denorm from settings group by pool) s2
        on s1.pool = s2.pool
        order by 1 asc , 3 desc, 2 asc
    ) s

    GROUP  BY 1
)
SELECT *
FROM final
WHERE LENGTH(label) < 35