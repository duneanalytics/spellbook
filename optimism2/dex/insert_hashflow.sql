CREATE OR REPLACE FUNCTION dex.insert_hashflow(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH aux as (
    SELECT
        hf.block_time,
        maker_symbol AS token_a_symbol,
        taker_symbol AS token_b_symbol,
        maker_token_amount AS token_a_amount,
        taker_token_amount AS token_b_amount,
        'hashflow' as project,
        '1' as version,
        'DEX' as category,
        pool as trader_a,
        trader as trader_b,
        maker_token_amount * 10 ^ erc20a.decimals as token_a_amount_raw,
        taker_token_amount * 10 ^ erc20b.decimals as token_b_amount_raw,
        usd_amount,
        maker_token as token_a_address,
        taker_token as token_b_address,
        router_contract as exchange_contract_address,
        tx_hash,
        trader as tx_from, -- already join on tx table with same logic in table creation
        router_contract as tx_to, -- already join on tx table with same logic in table creation
        NULL::integer[] as trace_address,
        (case when hf.composite_index=-1 then NULL::integer else hf.composite_index end) as evt_index -- -1 means decoded from traces
    FROM hashflow.trades hf

    LEFT JOIN erc20.tokens erc20a ON erc20a.contract_address = hf.maker_token
    LEFT JOIN erc20.tokens erc20b ON erc20b.contract_address = hf.taker_token
    WHERE fill_status is true -- success trade
          AND hf.block_time >= start_ts
          AND hf.block_time < end_ts
), rows AS (
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
        *,
        row_number() OVER (PARTITION BY project, tx_hash, evt_index, trace_address ORDER BY version, category) AS trade_id
    FROM aux
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


-- fill 2022
SELECT dex.insert_hashflow(
    '2022-05-07',
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-12-28'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'hashflow'
);

INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT dex.insert_hashflow(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project = 'hashflow'),
        now()
        );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;