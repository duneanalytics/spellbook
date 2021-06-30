with pools as (
    select c."poolId" as pool_id, unnest(cc.tokens) as token_address, unnest(cc.weights)/1e18 as normalized_weight
    from balancer_v2."Vault_evt_PoolRegistered" c
    inner join balancer_v2."WeightedPoolFactory_call_create" cc
    on c.evt_tx_hash = cc.call_tx_hash
    
    union all
    
    select c."poolId" as pool_id, unnest(cc.tokens) as token_address, unnest(cc.weights)/1e18 as normalized_weight
    from balancer_v2."Vault_evt_PoolRegistered" c
    inner join balancer_v2."WeightedPool2TokensFactory_call_create" cc
    on c.evt_tx_hash = cc.call_tx_hash
),
settings as (
    select pool_id, 
    coalesce(t.symbol,'?') as symbol, 
    normalized_weight
    from pools p
    left join erc20.tokens t on p.token_address = t.contract_address
)
SELECT 
  SUBSTRING(pool_id FOR 20) as address, 
  lower(CONCAT(string_agg(symbol, '/'), ' ', string_agg(cast(norm_weight as text), '/'))) AS label,
  'balancer_v2_pool' AS type,
  'balancerlabs' as author

FROM   (
    select s1.pool_id, symbol, cast(100*normalized_weight as integer) as norm_weight from settings s1
    order by 1 asc , 3 desc, 2 asc
) s
GROUP  BY 1
