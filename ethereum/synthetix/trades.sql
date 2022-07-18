CREATE TABLE synthetix.trades (
    block_time timestamptz NOT NULL,
    token_a_amount numeric,
    token_b_amount numeric,
    token_a_amount_usd numeric,
    token_b_amount_usd numeric,
    project text NOT NULL,
    version text,
    trader_a bytea,
    trader_b bytea,
    token_a_amount_raw numeric,
    token_b_amount_raw numeric,
    token_a_address bytea,
    token_b_address bytea,
    exchange_contract_address bytea NOT NULL,
    tx_hash bytea NOT NULL,
    trace_address integer[],
    evt_index integer,
    trade_id integer
);

CREATE UNIQUE INDEX IF NOT EXISTS synth_trades_evt_index_uniq_idx ON synthetix.trades (tx_hash, evt_index, trade_id);
CREATE INDEX IF NOT EXISTS synth_trades_block_time_idx ON synthetix.trades USING BRIN (block_time);

CREATE OR REPLACE FUNCTION synthetix.insert_trades(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO synthetix.trades
    SELECT
        trade.evt_block_time AS block_time,
        trade."fromAmount"/1e18 AS token_a_amount,
        trade."toAmount"/1e18 AS token_b_amount,
        trade."fromAmount"/1e18 * (SELECT currency_rate FROM synthetix.rates WHERE trade."fromCurrencyKey" = currency_key AND block_time <= trade.evt_block_time ORDER BY block_time DESC LIMIT 1)/1e18 as token_a_amount_usd,
        trade."toAmount"/1e18 * (SELECT currency_rate FROM synthetix.rates WHERE trade."toCurrencyKey" = currency_key AND block_time <= trade.evt_block_time ORDER BY block_time DESC LIMIT 1)/1e18 as token_b_amount_usd,
        'Synthetix' AS project,
        '1' AS version,
        trade.account AS trader_a,
        NULL::bytea AS trader_b,
        trade."fromAmount" AS token_a_amount_raw,
        trade."toAmount" AS token_b_amount_raw,
        (SELECT address FROM synthetix.synths WHERE trade."fromCurrencyKey" = currency_key AND block_time <= trade.evt_block_time ORDER BY block_time DESC LIMIT 1) AS token_a_address,        
        (SELECT address FROM synthetix.synths WHERE trade."toCurrencyKey" = currency_key AND block_time <= trade.evt_block_time ORDER BY block_time DESC LIMIT 1) AS token_b_address,
        trade.contract_address AS exchange_contract_address,
        trade.evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        trade.evt_index AS evt_index,
        row_number() OVER (PARTITION BY evt_tx_hash, evt_index) AS trade_id
    FROM
        synthetix."SNX_evt_SynthExchange" trade
    WHERE evt_block_time >= start_ts
    AND evt_block_time < end_ts
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

--backfill command
SELECT synthetix.insert_trades((SELECT max(block_time) FROM synthetix.trades));


INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', 'SELECT synthetix.insert_trades((SELECT max(block_time) FROM synthetix.trades));')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;


