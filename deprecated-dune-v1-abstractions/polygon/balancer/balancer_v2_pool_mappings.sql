BEGIN;

CREATE OR REPLACE FUNCTION balancer.balancer_v2_pool_mappings() RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT into balancer.address_mappings (
        address,
        label,
        type,
        author
    )
    with pools as (
        select c."poolId" as pool_id, unnest(cc.tokens) as token_address, unnest(cc.weights)/1e18 as normalized_weight, cc.symbol, 'WP' as pool_type
        from balancer_v2."Vault_evt_PoolRegistered" c
        inner join balancer_v2."WeightedPoolFactory_call_create" cc
        on c.evt_tx_hash = cc.call_tx_hash

        union all

        select c."poolId" as pool_id, unnest(cc.tokens) as token_address, unnest(cc.weights)/1e18 as normalized_weight, cc.symbol, 'WP2T' as pool_type
        from balancer_v2."Vault_evt_PoolRegistered" c
        inner join balancer_v2."WeightedPool2TokensFactory_call_create" cc
        on c.evt_tx_hash = cc.call_tx_hash

        union all

        select c."poolId" as pool_id, unnest(cc.tokens) as token_address, 0 as normalized_weight, cc.symbol, 'SP' as pool_type
        from balancer_v2."Vault_evt_PoolRegistered" c
        inner join balancer_v2."StablePoolFactory_call_create" cc
        on c.evt_tx_hash = cc.call_tx_hash

        union all

        select c."poolId" as pool_id, unnest(cc.tokens) as token_address, 0 as normalized_weight, cc.symbol, 'LBP' as pool_type
        from balancer_v2."Vault_evt_PoolRegistered" c
        inner join balancer_v2."LiquidityBootstrappingPoolFactory_call_create" cc
        on c.evt_tx_hash = cc.call_tx_hash
    ),
    settings as (
        select pool_id,
        coalesce(t.symbol,'?') as token_symbol,
        normalized_weight,
        p.symbol as pool_symbol,
        p.pool_type
        from pools p
        left join erc20.tokens t on p.token_address = t.contract_address
    )
    SELECT
      SUBSTRING(pool_id FOR 20) as address,
      case when pool_type in ('SP', 'LBP') then lower(pool_symbol)
      else
      lower(CONCAT(string_agg(token_symbol, '/'), ' ', string_agg(cast(norm_weight as text), '/')))
      end AS label,
      'balancer_v2_pool' AS type,
      'balancerlabs' as author
    FROM   (
        select s1.pool_id, token_symbol, pool_symbol, cast(100*normalized_weight as integer) as norm_weight, pool_type from settings s1
        order by 1 asc , 3 desc, 2 asc
    ) s
    GROUP  BY pool_id, pool_symbol, pool_type
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- intial fill
SELECT balancer.balancer_v2_pool_mappings();

-- daily update
INSERT INTO cron.job (schedule, command)
VALUES ('2 22 * * *', $$SELECT balancer.balancer_v2_pool_mappings();$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

COMMIT;
