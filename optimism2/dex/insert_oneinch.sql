--use dex version to control for a specific implementation. Default = 0 means include all versions.

CREATE OR REPLACE FUNCTION dex.insert_oneinch(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18, dex_version integer=0) RETURNS integer
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
            token_a_amount_raw / 10 ^ pa.decimals * pa.median_price,
            token_b_amount_raw / 10 ^ pb.decimals * pb.median_price
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
	   SELECT
            oiv.block_time AS block_time,
            '1inch' AS project,
            oiv.version AS version,
            'Aggregator' AS category,
            tx."from" AS trader_a,
            NULL::bytea AS trader_b,
            --Token a is what was received 
            oiv.to_amount AS token_a_amount_raw,
            oiv.from_amount AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
	    --map default eth to OP Eth dead address
            CASE WHEN to_token = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '\xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'::bytea ELSE to_token END AS token_a_address,
            CASE WHEN from_token = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '\xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'::bytea ELSE from_token END AS token_b_address,
            oiv.contract_address as exchange_contract_address,
            oiv.tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            oiv.evt_index
	    
        FROM ( --pulled from https://github.com/duneanalytics/abstractions/blob/master/ethereum/dex/trades/insert_1inch.sql
		--v3 router
		SELECT "srcToken" as from_token, "dstToken" as to_token, "spentAmount" as from_amount, "returnAmount" as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, NULL::integer[] as call_trace_address, evt_index, contract_address, '3' as version FROM oneinch."AggregationRouterV3_evt_Swapped"
			WHERE evt_block_time BETWEEN start_ts AND end_ts
		UNION ALL
		--v4 router
		SELECT decode(substring("desc"->>'srcToken' FROM 3), 'hex') as from_token, decode(substring("desc"->>'dstToken' FROM 3), 'hex') as to_token, "output_spentAmount" as from_amount, "output_returnAmount" as to_amount,
			call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address, NULL::integer as evt_index, contract_address, '4' as version FROM oneinch."AggregationRouterV4_call_swap" where call_success
			AND call_block_time BETWEEN start_ts AND end_ts
		) oiv
	
	INNER join optimism.transactions tx on tx.hash = oiv.tx_hash


        WHERE oiv.block_time >= start_ts AND oiv.block_time < end_ts

    ) dexs
    INNER JOIN optimism.transactions tx
        ON dexs.tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN erc20.tokens erc20a ON erc20a.contract_address = dexs.token_a_address
    LEFT JOIN erc20.tokens erc20b ON erc20b.contract_address = dexs.token_b_address
    LEFT JOIN prices.approx_prices_from_dex_data pa
      ON pa.hour = date_trunc('hour', dexs.block_time)
        AND pa.contract_address = dexs.token_a_address
        AND pa.hour >= start_ts
        AND pa.hour < end_ts
    LEFT JOIN prices.approx_prices_from_dex_data pb
      ON pb.hour = date_trunc('hour', dexs.block_time)
        AND pb.contract_address = dexs.token_b_address
        AND pb.hour >= start_ts
        AND pb.hour < end_ts
	
	WHERE 1 = (
		CASE WHEN dex_version = 0 THEN 1
		WHEN dexs.version::INTEGER = dex_version THEN 1
		ELSE 0
		END )
	
    -- update if we have new info on prices or the erc20
    ON CONFLICT (project, tx_hash, evt_index, trade_id)
    DO UPDATE SET
        usd_amount = EXCLUDED.usd_amount,
        token_a_amount = EXCLUDED.token_a_amount,
        token_b_amount = EXCLUDED.token_b_amount,
        token_a_symbol = EXCLUDED.token_a_symbol,
        token_b_symbol = EXCLUDED.token_b_symbol
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- table start fill
-- v3
SELECT dex.insert_oneinch(
    '2021-11-11',
    now(),
    0,
    (SELECT MAX(number) FROM optimism.blocks where time < now() - interval '20 minutes'),
	3
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-11-10'
    AND block_time <= now() - interval '20 minutes'
    AND project = '1inch' AND version = '3'
);
--v4
SELECT dex.insert_oneinch(
    '2021-11-11',
    now(),
    0,
    (SELECT MAX(number) FROM optimism.blocks where time < now() - interval '20 minutes'),
	4
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-11-10'
    AND block_time <= now() - interval '20 minutes'
    AND project = '1inch' AND version = '4'
);

INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT dex.insert_oneinch(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='1inch'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM optimism.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='1inch')),
        (SELECT MAX(number) FROM optimism.blocks where time < now() - interval '20 minutes'),
    0);
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
