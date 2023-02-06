with events as (
    -- binds
    select call_block_number as block_number, 
    contract_address as pool, token, denorm
    from balancer_v1_ethereum.BPool_call_bind
    where call_success

    union all

    -- rebinds
    select call_block_number as block_number, 
    contract_address as pool, token, denorm
    from balancer_v1_ethereum.BPool_call_rebind
    where call_success

    union all
    
    -- unbinds
    select call_block_number as block_number, 
    contract_address as pool, token, 0 as denorm
    from balancer_v1_ethereum.BPool_call_unbind
    where call_success
),

state_with_gaps as (
    select events.block_number, events.pool, events.token, CAST(events.denorm AS double),
    LEAD(events.block_number, 1, 99999999) over (partition by events.pool, events.token order by events.block_number) as next_block_number
    from events
),

settings as (
    select pool, 
    coalesce(t.symbol,'?') as symbol, 
    denorm,
    next_block_number
    from state_with_gaps s
    left join tokens.erc20 t on s.token = t.contract_address
    AND blockchain = "ethereum"
    where next_block_number = 99999999
    and denorm > 0
),

final as (
    SELECT 
      array('ethereum') AS blockchain,
      pool as address,
      lower(concat(array_join(collect_list(symbol), '/'), ' ', array_join(collect_list(cast(norm_weight AS string)), '/'))) AS name,
      'balancer_v1_pool' AS category,
      'balancerlabs' AS contributor,
      'query' AS source,
      timestamp('2023-02-02') AS created_at,
      now() AS updated_at

    FROM   (
        select s1.pool, symbol, cast(100*denorm/total_denorm as integer) as norm_weight from settings s1
        inner join (select pool, sum(denorm) as total_denorm from settings group by pool) s2
        on s1.pool = s2.pool
        order by 1 asc , 3 desc, 2 asc
    ) s

    GROUP BY 1, 2
)
SELECT *
FROM final
WHERE LENGTH(name) < 35