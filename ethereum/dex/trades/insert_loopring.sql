CREATE OR REPLACE FUNCTION dex.insert_loopring(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO dex.trades (
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
        tx_hash,
        tx."from" as tx_from,
        tx."to" as tx_to,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY project, tx_hash, evt_index, trace_address ORDER BY version, category) AS trade_id
    FROM (

        -- Loopring v3.1
        (
            WITH trades AS (
                SELECT loopring.fn_process_trade_block_v1(CAST(b."blockSize" AS INT), b._3, b.call_block_time) AS trade,
                    b."contract_address" AS exchange_contract_address,
                    b.call_tx_hash AS tx_hash,
                    b.call_trace_address AS trace_address,
                    NULL::bigint AS evt_index
                FROM loopring."DEXBetaV1_call_commitBlock" b
                WHERE b."blockType" = '0'
            ), token_table AS (
                SELECT 0 AS "token_id", '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS token
                UNION
                SELECT "tokenId" AS "token_id", "token"
                FROM loopring."DEXBetaV1_evt_TokenRegistered" e
                WHERE token != '\x0000000000000000000000000000000000000000'
            )
            SELECT (t.trade).block_timestamp AS block_time,
                'Loopring' AS project,
                '3.1' AS version,
                'DEX' AS category,
                (SELECT "owner" FROM loopring."DEXBetaV1_evt_AccountCreated" WHERE "id" = (t.trade).accountA) AS trader_a,
                (SELECT "owner" FROM loopring."DEXBetaV1_evt_AccountCreated" WHERE "id" = (t.trade).accountB) AS trader_B,
                (t.trade).fillA::numeric AS token_a_amount_raw,
                (t.trade).fillB::numeric AS token_b_amount_raw,
                NULL::numeric AS usd_amount,
                (SELECT "token" FROM token_table WHERE "token_id" = (t.trade).tokenA) AS token_a_address,
                (SELECT "token" FROM token_table WHERE "token_id" = (t.trade).tokenB) AS token_b_address,
                exchange_contract_address,
                tx_hash,
                trace_address,
                evt_index
            FROM trades t
        )

        UNION ALL

        -- Loopring v3.6
        (
            WITH transactions AS (
                SELECT loopring.fn_process_block_v2(
                    CAST(t.block ->> 'blockSize' AS INT),
                    decode(substring(t.block ->> 'data', 3, char_length(t.block ->> 'data') - 2), 'hex'),
                    c.call_block_time,
                    blockIdx::integer
                ) as transaction,
                c."contract_address" AS exchange_contract_address,
                c.call_tx_hash AS tx_hash,
                c.call_trace_address AS trace_address,
                NULL::bigint AS evt_index
                FROM loopring."ExchangeV3_call_submitBlocks" c,
                jsonb_array_elements(c."blocks") with ordinality as t(block, blockIdx)
            ), token_table AS (
                SELECT 0 AS "token_id", '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS token
                UNION
                SELECT "tokenId" AS "token_id", "token"
                FROM loopring."ExchangeV3_evt_TokenRegistered" e
                WHERE token != '\x0000000000000000000000000000000000000000'
            ), _account_table AS (
                SELECT CASE (t.transaction).txType
                            WHEN 1 THEN ((t.transaction).deposit).toAccount
                            WHEN 3 THEN ((t.transaction).transfer).toAccount
                            WHEN 5 THEN ((t.transaction).account_update).ownerAccount
                            ELSE '0'
                        END as id,
                        CASE (t.transaction).txType
                            WHEN 1 THEN ((t.transaction).deposit).toAddress
                            WHEN 3 THEN ((t.transaction).transfer).toAddress
                            WHEN 5 THEN ((t.transaction).account_update).ownerAddress
                            ELSE '\x0000000000000000000000000000000000000000'::bytea
                        END as address
                FROM transactions t
            ), account_table AS (
                SELECT DISTINCT id, address
                FROM _account_table
                WHERE id != 0 AND address != '\x0000000000000000000000000000000000000000'::bytea
            )
            SELECT (t.transaction).block_timestamp AS block_time,
                'Loopring' AS project,
                '3.6' AS version,
                'DEX' AS category,
                (SELECT "address" FROM account_table WHERE "id" = ((t.transaction).spot_trade).accountA) AS trader_a,
                (SELECT "address" FROM account_table WHERE "id" = ((t.transaction).spot_trade).accountB) AS trader_B,
                ((t.transaction).spot_trade).amountA::numeric AS token_a_amount_raw,
                ((t.transaction).spot_trade).amountB::numeric AS token_b_amount_raw,
                NULL::numeric AS usd_amount,
                (SELECT "token" FROM token_table WHERE "token_id" = ((t.transaction).spot_trade).tokenA) AS token_a_address,
                (SELECT "token" FROM token_table WHERE "token_id" = ((t.transaction).spot_trade).tokenB) AS token_b_address,
                exchange_contract_address,
                tx_hash,
                trace_address,
                evt_index
            FROM transactions t
            WHERE (t.transaction).txType = 4
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
SELECT dex.insert_loopring(
    '2020-01-01',
    '2021-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2020-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2021-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2020-01-01'
    AND block_time <= '2021-01-01'
    AND project = 'Loopring'
);

-- fill 2021
SELECT dex.insert_loopring(
    '2021-01-01',
    now(),
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-01-01'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'Loopring'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/12 * * * *', $$
    SELECT dex.insert_loopring(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Loopring'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Loopring')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;