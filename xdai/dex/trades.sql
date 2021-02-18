CREATE TABLE dex.trades (
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
    tx_hash bytea NOT NULL,
    tx_from bytea NOT NULL,
    tx_to bytea,
    trace_address integer[],
    evt_index integer,
    trade_id integer
);

CREATE OR REPLACE FUNCTION dex.insert_trades(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
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
    -- Perpetual Protocol (It has its own USD-prices)
    SELECT
        p.evt_block_time AS block_time,
        CASE
            WHEN p."exchangedPositionSize" >= 0 THEN amm.base_symbol
            ELSE amm.quote_symbol
        END AS token_a_symbol,
        CASE
            WHEN p."exchangedPositionSize" < 0 THEN amm.base_symbol
            ELSE amm.quote_symbol
        END AS token_b_symbol,
        CASE
            WHEN p."exchangedPositionSize" >= 0 THEN ABS(p."exchangedPositionSize") / 10^18
            ELSE p."positionNotional" / 10^18
        END AS token_a_amount,
        CASE
            WHEN p."exchangedPositionSize" < 0 THEN ABS(p."exchangedPositionSize") / 10^18
            ELSE p."positionNotional" / 10^18
        END AS token_b_amount,
        'Perpetual' AS project,
        1 AS version,
        'DEX' AS category,
        p.trader AS trader_a,
        NULL AS trader_b,
        CASE
            WHEN p."exchangedPositionSize" >= 0 THEN ABS(p."exchangedPositionSize")
            ELSE p."positionNotional" * 10^(amm.quote_token_decimals - 18)
        END AS token_a_amount_raw,
        CASE
            WHEN p."exchangedPositionSize" < 0 THEN ABS(p."exchangedPositionSize")
            ELSE p."positionNotional" * 10^(amm.quote_token_decimals - 18)
        END AS token_b_amount_raw,
        p."positionNotional" / 10^18 AS usd_amount,
        CASE
            WHEN p."exchangedPositionSize" >= 0 THEN NULL
            ELSE amm.quote_token_address
        END AS token_a_address,
        CASE
            WHEN p."exchangedPositionSize" < 0 THEN NULL
            ELSE amm.quote_token_address
        END AS token_b_address,
        p.contract_address AS exchange_contract_address,
        p.evt_tx_hash AS tx_hash,
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        NULL AS trace_address,
        p.evt_index AS evt_index,
        row_number() OVER (PARTITION BY p.evt_tx_hash, p.evt_index) AS trade_id
    FROM perp."ClearingHouse_evt_PositionChanged" p
    INNER JOIN perp.view_amm amm
        ON p.amm = amm.contract_address
    INNER JOIN xdai.transactions tx
        ON p.evt_tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    WHERE p.evt_block_time >= start_ts
    AND p.evt_block_time < end_ts

    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;


CREATE UNIQUE INDEX IF NOT EXISTS dex_trades_tr_addr_uniq_idx ON dex.trades (tx_hash, trace_address, trade_id);
CREATE UNIQUE INDEX IF NOT EXISTS dex_trades_evt_index_uniq_idx ON dex.trades (tx_hash, evt_index, trade_id);
CREATE INDEX IF NOT EXISTS dex_trades_tx_from_idx ON dex.trades (tx_from);
CREATE INDEX IF NOT EXISTS dex_trades_tx_to_idx ON dex.trades (tx_to);
CREATE INDEX IF NOT EXISTS dex_trades_project_idx ON dex.trades (project);
CREATE INDEX IF NOT EXISTS dex_trades_block_time_idx ON dex.trades USING BRIN (block_time);
CREATE INDEX IF NOT EXISTS dex_trades_token_a_idx ON dex.trades (token_a_address);
CREATE INDEX IF NOT EXISTS dex_trades_token_b_idx ON dex.trades (token_b_address);

-- fill 2017
SELECT dex.insert_trades(
    '2017-01-01',
    '2018-01-01',
    (SELECT max(number) FROM xdai.blocks WHERE time < '2017-01-01'),
    (SELECT max(number) FROM xdai.blocks WHERE time <= '2018-01-01'))
WHERE NOT EXISTS (SELECT * FROM dex.trades WHERE block_time > '2017-01-01' AND block_time <= '2018-01-01' LIMIT 1);

-- fill 2018
SELECT dex.insert_trades(
    '2018-01-01',
    '2019-01-01',
    (SELECT max(number) FROM xdai.blocks WHERE time < '2018-01-01'),
    (SELECT max(number) FROM xdai.blocks WHERE time <= '2019-01-01'))
WHERE NOT EXISTS (SELECT * FROM dex.trades WHERE block_time > '2018-01-01' AND block_time <= '2019-01-01' LIMIT 1);

-- fill 2019 H1
SELECT dex.insert_trades(
    '2019-01-01',
    '2019-07-01',
    (SELECT max(number) FROM xdai.blocks WHERE time < '2019-01-01'),
    (SELECT max(number) FROM xdai.blocks WHERE time <= '2019-07-01'))
WHERE NOT EXISTS (SELECT * FROM dex.trades WHERE block_time > '2019-01-01' AND block_time <= '2019-07-01' LIMIT 1);

-- fill 2019 H2
SELECT dex.insert_trades(
    '2019-07-01',
    '2020-01-01',
    (SELECT max(number) FROM xdai.blocks WHERE time <= '2019-07-01'),
    (SELECT max(number) FROM xdai.blocks WHERE time < '2020-01-01'))
WHERE NOT EXISTS (SELECT * FROM dex.trades WHERE block_time > '2019-07-01' AND block_time <= '2020-01-01' LIMIT 1);

-- fill 2020 H1
SELECT dex.insert_trades(
    '2020-01-01',
    '2020-07-01',
    (SELECT max(number) FROM xdai.blocks WHERE time <= '2020-01-01'),
    (SELECT max(number) FROM xdai.blocks WHERE time <= '2020-07-01'))
WHERE NOT EXISTS (SELECT * FROM dex.trades WHERE block_time > '2020-01-01' AND block_time <= '2020-07-01' LIMIT 1);

-- fill 2020 H2
SELECT dex.insert_trades(
    '2020-07-01',
    '2021-01-01',
    (SELECT max(number) FROM xdai.blocks WHERE time <= '2020-07-01'),
    (SELECT max(number) FROM xdai.blocks WHERE time <= '2021-01-01'))
WHERE NOT EXISTS (SELECT * FROM dex.trades WHERE block_time > '2020-07-01' AND block_time <= '2021-01-01' LIMIT 1);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$SELECT dex.insert_trades((SELECT max(block_time) - interval '1 days' FROM dex.trades), (SELECT now()), (SELECT max(number) FROM xdai.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades)), (SELECT MAX(number) FROM xdai.blocks));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
