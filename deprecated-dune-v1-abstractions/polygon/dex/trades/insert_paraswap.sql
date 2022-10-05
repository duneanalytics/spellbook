CREATE OR REPLACE FUNCTION dex.insert_paraswap(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
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
        erc20a.symbol as token_a_symbol,
        erc20b.symbol as token_b_symbol,
        token_a_amount_raw / 10 ^ erc20a.decimals AS token_a_amount,
        token_b_amount_raw / 10 ^ erc20b.decimals AS token_b_amount,
        'Paraswap' as project,
        version_ as version,
        'Aggregator' as category,
        COALESCE(trader_a, tx."from") as trader_a,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        GREATEST(
            token_a_amount_raw / 10 ^ pa.decimals * pa.price,
            token_b_amount_raw / 10 ^ pb.decimals * pb.price
        ) AS usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx."from" as tx_from,
        tx."to" as tx_to,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY tx_hash, evt_index) AS trade_id
    FROM (
        SELECT
            swaps."evt_block_time" AS block_time,
            swaps."initiator" AS trader_a,
            swaps."beneficiary" AS trader_b,
            swaps."srcAmount" AS token_a_amount_raw,
            swaps."receivedAmount" AS token_b_amount_raw,
            swaps."srcToken" AS token_a_address,
            swaps."destToken" AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."evt_tx_hash" AS tx_hash,
            NULL::integer[] AS trace_address,
            swaps."evt_index" AS evt_index,
            swaps.version_ as version_
        FROM (
            SELECT '4.0.0' as version_, "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index" FROM paraswap."AugustusSwapperV4_evt_Bought" UNION ALL
            SELECT '4.0.0' as version_, "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index" FROM paraswap."AugustusSwapperV4_evt_Swapped" UNION ALL
            SELECT '5.0.0' as version_, "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index" FROM paraswap."AugustusSwapperV5_evt_Bought" UNION ALL
            SELECT '5.0.0' as version_, "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index" FROM paraswap."AugustusSwapperV5_evt_Swapped" UNION ALL
            SELECT '5.2.0' as version_, "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index" FROM paraswap."AugustusSwapperV5_evt_Bought2" UNION ALL
            SELECT '5.2.0' as version_, "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index" FROM paraswap."AugustusSwapperV5_evt_Swapped2" UNION ALL
            SELECT '5.3.0' as version_, "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index" FROM paraswap."AugustusSwapperV5_evt_BoughtV3" UNION ALL
            SELECT '5.3.0' as version_, "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index" FROM paraswap."AugustusSwapperV5_evt_SwappedV3"
        ) swaps
    ) dexs
    INNER JOIN polygon.transactions tx
        ON dexs.tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN erc20.tokens erc20a ON erc20a.contract_address = (
        CASE 
            WHEN dexs.token_a_address='\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '\x0000000000000000000000000000000000001010'
            ELSE dexs.token_a_address
        END
    )
    LEFT JOIN erc20.tokens erc20b ON erc20b.contract_address = (
        CASE 
            WHEN dexs.token_b_address='\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '\x0000000000000000000000000000000000001010'
            ELSE dexs.token_b_address
        END
    )
    LEFT JOIN prices.usd pa ON pa.minute = date_trunc('minute', dexs.block_time)
        AND pa.contract_address = (
            CASE 
                WHEN dexs.token_a_address='\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '\x0000000000000000000000000000000000001010'
                ELSE dexs.token_a_address
            END
        )
        AND pa.minute >= start_ts
        AND pa.minute < end_ts
    LEFT JOIN prices.usd pb ON pb.minute = date_trunc('minute', dexs.block_time)
        AND pb.contract_address = (
            CASE 
                WHEN dexs.token_b_address='\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '\x0000000000000000000000000000000000001010'
                ELSE dexs.token_b_address
            END
        ) 
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

-- fill 2021
SELECT dex.insert_paraswap(
    '2021-01-01',
    '2022-01-01',
    (SELECT max(number) FROM polygon.blocks WHERE time < '2021-01-01'),
    (SELECT max(number) FROM polygon.blocks WHERE time <= '2022-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-01-01'
    AND block_time <= '2022-01-01'
    AND project = 'Paraswap'
);

-- fill 2022
SELECT dex.insert_paraswap(
    '2022-01-01',
    now(),
    (SELECT max(number) FROM polygon.blocks WHERE time < '2022-01-01'),
    (SELECT MAX(number) FROM polygon.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2022-01-01'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'Paraswap'
);


INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_paraswap(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Paraswap'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM polygon.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Paraswap')),
        (SELECT MAX(number) FROM polygon.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;