CREATE TABLE synthetix.view_trades (
    block_time timestamptz NOT NULL,
    token_a_amount numeric,
    token_b_amount numeric,
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

CREATE OR REPLACE FUNCTION synthetix.insert_trades(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO synthetix.view_trades
    SELECT
        trade.evt_block_time AS block_time,
        trade."fromAmount"/1e18 * (SELECT currency_rate FROM synthetix.view_rates WHERE trade."fromCurrencyKey" = currency_key AND evt_block_time <= trade.evt_block_time ORDER BY evt_block_time DESC LIMIT 1)/1e18 as token_a_amount,
        trade."toAmount"/1e18 * (SELECT currency_rate FROM synthetix.view_rates WHERE trade."toCurrencyKey" = currency_key AND evt_block_time <= trade.evt_block_time ORDER BY evt_block_time DESC LIMIT 1)/1e18 as token_b_amount,
        'Synthetix' AS project,
        '1' AS version,
        trade.account AS trader_a,
        NULL::bytea AS trader_b,
        trade."fromAmount"/1e18 AS token_a_amount_raw,
        trade."toAmount"/1e18 AS token_b_amount_raw,
        (SELECT address FROM synthetix.view_synths WHERE trade."fromCurrencyKey" = currency_key AND evt_block_time <= trade.evt_block_time ORDER BY evt_block_time DESC LIMIT 1) AS token_a_address,        
        (SELECT address FROM synthetix.view_synths WHERE trade."toCurrencyKey" = currency_key AND evt_block_time <= trade.evt_block_time ORDER BY evt_block_time DESC LIMIT 1) AS token_b_address,
        trade.contract_address AS exchange_contract_address,
        trade.evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        trade.evt_index AS evt_index,
        row_number() OVER (PARTITION BY tx_hash, evt_index) AS trade_id
    FROM
        synthetix."Synthetix_evt_SynthExchange" trade
    WHERE block_time >= start_ts
    AND block_time < end_ts
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

CREATE UNIQUE INDEX IF NOT EXISTS synthetix_view_trades_id ON synthetix.view_trades (tx_hash,evt_index);
CREATE INDEX synthetix_view_trades_block_time ON synthetix.view_trades (block_time);
CREATE INDEX synthetix_view_trades_token_a_address ON synthetix.view_trades (token_a_address);
CREATE INDEX synthetix_view_trades_token_b_address ON synthetix.view_trades (token_b_address);

INSERT INTO cron.job (schedule, command)
VALUES ('0,10,20,30,40,50 * * * *', 'SELECT synthetix.insert_trades((SELECT max(block_time) FROM synthetix.view_trades));')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;


