CREATE OR REPLACE FUNCTION dex.insert_liquidity_balancer_v2(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH days as ( -- update table entries until previous day
    SELECT day FROM generate_series(start_ts, (SELECT end_ts - interval '1 day'), '1 day') g(day)
),
balancer_v2_pools as ( -- https://github.com/duneanalytics/abstractions/blob/master/labels/ethereum/balancer_v2_pools.sql
    SELECT
        pool_id,
        SUBSTRING(pool_id FOR 20) as pool_address,
        token_address,
        normalized_weight,
        symbol as pool_name_symbolic
    FROM
    (
    select c."poolId" as pool_id, unnest(cc.tokens) as token_address, unnest(cc.weights)/1e18 as normalized_weight, cc.symbol, 'WP' as pool_type
    from balancer_v2."Vault_evt_PoolRegistered" c
    inner join balancer_v2."WeightedPoolFactory_call_create" cc
    on c.evt_tx_hash = cc.call_tx_hash
    union all
    select c."poolId" as pool_id, unnest(cc.tokens) as token_address, unnest(cc.weights)/1e18 as normalized_weight, cc.symbol, 'WP2T' as pool_type
    from balancer_v2."Vault_evt_PoolRegistered" c
    inner join balancer_v2."WeightedPool2TokensFactory_call_create" cc
    on c.evt_tx_hash = cc.call_tx_hash
    ) all_pools
),
balance_changes AS (
SELECT
    "poolId" AS pool_id,
    unnest(tokens) AS token_address,
    unnest(deltas) AS liq_change,
    evt_block_time,
    'token_x' AS token_index
FROM balancer_v2."Vault_evt_PoolBalanceChanged"
UNION ALL
SELECT 
    "poolId",
    "tokenIn", 
    "amountIn",
    evt_block_time,
    'token_x' AS token_index
FROM balancer_v2."Vault_evt_Swap"
UNION ALL
SELECT  
    "poolId",
    "tokenOut", 
    - "amountOut",
    evt_block_time,
    'token_x' AS token_index
FROM balancer_v2."Vault_evt_Swap"
UNION ALL
SELECT  
    "poolId",
    token, 
    "cashDelta" + "managedDelta",
    evt_block_time,
    'token_x' AS token_index
FROM balancer_v2."Vault_evt_PoolBalanceManaged"
),
dex_wallet_balances AS (
    SELECT
        change.pool_id AS wallet_address,
        change.token_address,
        SUM(liq_change) OVER (PARTITION BY change.pool_id, change.token_address ORDER BY evt_block_time) AS amount_raw,
        change.evt_block_time AS timestamp,
        token_index
    FROM balance_changes change
),
balances AS ( -- logic from https://github.com/duneanalytics/abstractions/pull/398
    SELECT
        wallet_address,
        token_address,
        token_index,
        amount_raw,
        date_trunc('day', timestamp) as day,
        lead(date_trunc('day', timestamp), 1, now()) OVER (PARTITION BY token_address, wallet_address, token_index ORDER BY timestamp) AS next_day
        FROM dex_wallet_balances
),
rows AS (
    INSERT INTO dex.liquidity (
        day,
        token_symbol,
        token_amount,
        pool_name,
        project,
        version,
        category,
        token_amount_raw,
        token_usd_amount,
        token_address,
        pool_address,
        token_index,
        token_pool_percentage
    )
    SELECT
        day,
        erc20.symbol AS token_symbol,
        token_amount_raw / 10 ^ erc20.decimals AS token_amount,
        pool_name,
        project,
        version,
        category,
        token_amount_raw,
        token_amount_raw / (10 ^ erc20.decimals) * p.price AS usd_amount,
        token_address,
        pool_address,
        token_index,
        token_pool_percentage
    FROM (
        -- Balancer v2
        SELECT
            d.day,
            (labels.get(SUBSTRING(balances.wallet_address FOR 20), 'lp_pool_name'))[1] AS pool_name,
            'Balancer' AS project,
            '2' AS version,
            'DEX' AS category,
            balances.amount_raw AS token_amount_raw,
            balances.token_address,
            SUBSTRING(balances.wallet_address FOR 20) AS pool_address,
            balances.token_index,
            pools.normalized_weight AS token_pool_percentage
        FROM balances
        INNER JOIN days d ON balances.day <= d.day AND d.day < balances.next_day
        LEFT JOIN balancer_v2_pools pools ON balances.wallet_address = pools.pool_id AND balances.token_address = pools.token_address
    ) dexs
    LEFT JOIN erc20.tokens erc20 on erc20.contract_address = dexs.token_address
    LEFT JOIN prices.usd p on p.contract_address = dexs.token_address and p.minute = dexs.day
        AND p.minute >= start_ts
        AND p.minute < end_ts

    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Balancer v2 vault contract deployed on '2021-04-19'
-- fill 2021 Q2 + Q3
SELECT dex.insert_liquidity_balancer_v2(
    '2021-04-01',
    now()
);

INSERT INTO cron.job (schedule, command)
VALUES ('23 3 * * *', $$
    SELECT dex.insert_liquidity_balancer_v2(
        (SELECT max(day) FROM dex.liquidity WHERE project = 'Balancer' and version = '2'),
        (SELECT now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
