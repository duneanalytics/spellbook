CREATE TABLE balancer.view_trades (
    block_time timestamptz NOT NULL,
    token_a_symbol text,
    token_b_symbol text,
    token_a_amount numeric,
    token_b_amount numeric,
    project text NOT NULL,
    version text,
    category text,
    trader_a bytea,
    trader_b bytea,
    token_a_amount_raw numeric,
    token_b_amount_raw numeric,
    usd_amount numeric,
    token_a_address bytea,
    token_b_address bytea,
    exchange_contract_address bytea NOT NULL,
    swap_fee numeric,
    tx_hash bytea NOT NULL,
    tx_from bytea NOT NULL,
    tx_to bytea,
    trace_address integer[],
    evt_index integer,
    trade_id integer
);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS balancer_trades_proj_tr_addr_uniq_idx ON balancer.view_trades (project, tx_hash, trace_address, trade_id);
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS balancer_trades_proj_evt_index_uniq_idx ON balancer.view_trades (project, tx_hash, evt_index, trade_id);
CREATE INDEX IF NOT EXISTS balancer_trades_tx_from_idx ON balancer.view_trades (tx_from);
CREATE INDEX IF NOT EXISTS balancer_trades_tx_to_idx ON balancer.view_trades (tx_to);
CREATE INDEX IF NOT EXISTS balancer_trades_project_idx ON balancer.view_trades (project);
CREATE INDEX IF NOT EXISTS balancer_trades_block_time_idx ON balancer.view_trades USING BRIN (block_time);
CREATE INDEX IF NOT EXISTS balancer_trades_token_a_idx ON balancer.view_trades (token_a_address);
CREATE INDEX IF NOT EXISTS balancer_trades_token_b_idx ON balancer.view_trades (token_b_address);
CREATE INDEX IF NOT EXISTS balancer_trades_block_time_project_idx ON balancer.view_trades (block_time, project);

CREATE OR REPLACE FUNCTION balancer.insert_trades(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO balancer.view_trades (
        block_time,
        token_a_symbol,
        token_b_symbol,
        token_a_amount,
        token_b_amount,
        project,
        version,
        category,
        trader_a,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        swap_fee,
        tx_hash,
        tx_from,
        tx_to,
        trace_address,
        evt_index,
        trade_id
    )
    SELECT
        dexs.block_time,
        erc20a.symbol AS token_a_symbol,
        erc20b.symbol AS token_b_symbol,
        token_a_amount_raw / 10 ^ erc20a.decimals AS token_a_amount,
        token_b_amount_raw / 10 ^ erc20b.decimals AS token_b_amount,
        project,
        version,
        category,
        coalesce(trader_a, tx."from") as trader_a, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        coalesce(
            usd_amount,
            token_a_amount_raw / 10 ^ pa.decimals * pa.price,
            token_b_amount_raw / 10 ^ pb.decimals * pb.price
        ) as usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        swap_fee,
        tx_hash,
        tx."from" as tx_from,
        tx."to" as tx_to,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY project, tx_hash, evt_index, trace_address ORDER BY version, category) AS trade_id
    FROM (
        -- V1
        SELECT
            t.evt_block_time AS block_time,
            'Balancer' AS project,
            '1' AS version,
            'DEX' AS category,
            NULL::bytea AS trader_a, -- this relies on the outer query coalescing to tx."from"
            NULL::bytea AS trader_b,
            t."tokenAmountOut" AS token_a_amount_raw,
            t."tokenAmountIn" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            t."tokenOut" token_a_address,
            t."tokenIn" token_b_address,
            t.contract_address exchange_contract_address,
            s."swapFee"/1e18 AS swap_fee,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM
            balancer."BPool_evt_LOG_SWAP" t
        INNER JOIN balancer."BFactory_evt_LOG_NEW_POOL" f ON f.pool = t.contract_address
        LEFT JOIN balancer_v1.view_pools_fees s
        ON s.contract_address = t.contract_address
        AND s.call_block_time = (
            SELECT MAX(call_block_time)
            FROM balancer_v1.view_pools_fees
            WHERE call_block_time <= t.evt_block_time
            AND contract_address = t.contract_address
            AND call_success
        )

        UNION ALL

        -- V2
        SELECT
            t.evt_block_time AS block_time,
            'Balancer' AS project,
            '2' AS version,
            'DEX' AS category,
            NULL::bytea AS trader_a, -- this relies on the outer query coalescing to tx."from"
            NULL::bytea AS trader_b,
            t."amountOut" AS token_a_amount_raw,
            t."amountIn" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            t."tokenOut" AS token_a_address,
            t."tokenIn" AS token_b_address,
            t."poolId" AS exchange_contract_address,
            s."swapFeePercentage"/1e18 AS swap_fee,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM
            balancer_v2."Vault_evt_Swap" t
        LEFT JOIN balancer_v2.view_pools_fees s
        ON s.contract_address = SUBSTRING(t."poolId" from 0 for 21)
        AND s.evt_block_time = (
            SELECT MAX(evt_block_time)
            FROM balancer_v2.view_pools_fees
            WHERE evt_block_time <= t.evt_block_time
            AND contract_address = SUBSTRING(t."poolId" from 0 for 21)
        )
    ) dexs
    INNER JOIN ethereum.transactions tx
        ON dexs.tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN erc20.tokens erc20a ON erc20a.contract_address = dexs.token_a_address
    LEFT JOIN erc20.tokens erc20b ON erc20b.contract_address = dexs.token_b_address
    LEFT JOIN prices.usd pa ON pa.minute = date_trunc('minute', dexs.block_time)
        AND pa.contract_address = dexs.token_a_address
        AND pa.minute >= start_ts
        AND pa.minute < end_ts
    LEFT JOIN prices.usd pb ON pb.minute = date_trunc('minute', dexs.block_time)
        AND pb.contract_address = dexs.token_b_address
        AND pb.minute >= start_ts
        AND pb.minute < end_ts
    WHERE dexs.block_time >= start_ts
    AND dexs.block_time < end_ts
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2020
SELECT balancer.insert_trades(
    '2020-01-01',
    '2021-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2020-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2021-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM balancer.view_trades
    WHERE block_time > '2020-01-01'
    AND block_time <= '2021-01-01'
    AND project = 'Balancer'
);

-- fill 2021
SELECT balancer.insert_trades(
    '2021-01-01',
    now(),
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM balancer.view_trades
    WHERE block_time > '2021-01-01'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'Balancer'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT balancer.insert_trades(
        (SELECT max(block_time) - interval '1 days' FROM balancer.view_trades WHERE project='Balancer'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM balancer.view_trades WHERE project='Balancer')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;