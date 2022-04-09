CREATE OR REPLACE FUNCTION dex.insert_saddle(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH

FlashLoanTokens AS (        
	SELECT contract_address, "tokenAddress", output_0 as index, count(1)
	FROM saddle."SwapFlashLoan_call_getTokenIndex"
	where call_success = true
	group by contract_address, "tokenAddress", output_0
)
,MetaSwapTokens AS (
	SELECT contract_address, index, output_0 as "tokenAddress", count(1)
	FROM saddle."MetaSwap_call_getToken"
	where call_success = true
	group by contract_address, index, output_0
)
,SwapUtilsTokens AS (
	SELECT 0 as index, '\x8dAEBADE922dF735c38C80C7eBD708Af50815fAa'::bytea as "tokenAddress", '\x4f6A43Ad7cba042606dECaCA730d4CE0A57ac62e'::bytea as contract_address union all
	SELECT 1 as index, '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea as "tokenAddress", '\x4f6A43Ad7cba042606dECaCA730d4CE0A57ac62e'::bytea as contract_address union all
	SELECT 2 as index, '\xEB4C2781e4ebA804CE9a9803C67d0893436bB27D'::bytea as "tokenAddress", '\x4f6A43Ad7cba042606dECaCA730d4CE0A57ac62e'::bytea as contract_address union all
	SELECT 3 as index, '\xfE18be6b3Bd88A2D2A7f928d00292E7a9963CfC6'::bytea as "tokenAddress", '\x4f6A43Ad7cba042606dECaCA730d4CE0A57ac62e'::bytea as contract_address 
),
 rows AS (
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

        -- Flash
        SELECT
        swap.evt_block_time AS block_time,
            'Saddle' AS project,
            '1' AS version,
            'DEX' AS category,
            swap."buyer" AS trader_a,
            NULL::bytea AS trader_b,
            "tokensBought" AS token_a_amount_raw,
            "tokensSold" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            tb."tokenAddress" AS token_a_address,
            ts."tokenAddress" AS token_b_address,
            swap.contract_address AS exchange_contract_address,
            swap.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            swap.evt_index
        FROM
            saddle."SwapFlashLoan_evt_TokenSwap" swap
        inner join FlashLoanTokens as tb on (swap."boughtId" = tb.index and swap.contract_address = tb.contract_address)
        inner join FlashLoanTokens as ts on (swap."soldId" = ts.index and swap.contract_address = ts.contract_address)

        UNION ALL
        -- Utils
        SELECT
        swap.evt_block_time AS block_time,
            'Saddle' AS project,
            '1' AS version,
            'DEX' AS category,
            swap."buyer" AS trader_a,
            NULL::bytea AS trader_b,
            "tokensBought" AS token_a_amount_raw,
            "tokensSold" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            tb."tokenAddress" AS token_a_address,
            ts."tokenAddress" AS token_b_address,
            swap.contract_address AS exchange_contract_address,
            swap.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            swap.evt_index
        FROM
        saddle."SwapUtils_evt_TokenSwap" as swap
        inner join SwapUtilsTokens as tb on (swap."boughtId" = tb.index and swap.contract_address = tb.contract_address)
        inner join SwapUtilsTokens as ts on (swap."soldId" = ts.index and swap.contract_address = ts.contract_address)
        
        UNION ALL
        -- Meta
        SELECT
		swap.evt_block_time AS block_time,
            'Saddle' AS project,
            '1' AS version,
            'DEX' AS category,
            swap."buyer" AS trader_a,
            NULL::bytea AS trader_b,
            "tokensBought" AS token_a_amount_raw,
            "tokensSold" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            tb."tokenAddress" AS token_a_address,
            ts."tokenAddress" AS token_b_address,
            swap.contract_address AS exchange_contract_address,
            swap.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            swap.evt_index
        FROM
            saddle."MetaSwap_evt_TokenSwapUnderlying" swap
        inner join MetaSwapTokens as tb on (swap."boughtId" = tb.index and swap.contract_address = tb.contract_address)
        inner join MetaSwapTokens as ts on (swap."soldId" = ts.index and swap.contract_address = ts.contract_address)

        
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
SELECT dex.insert_saddle(
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
    AND project = 'Saddle'
);

-- fill 2021
SELECT dex.insert_saddle(
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
    AND project = 'Saddle'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_saddle(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Saddle'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Saddle')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
