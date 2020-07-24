BEGIN;
DROP MATERIALIZED VIEW IF EXISTS synthetix.view_trades;
CREATE MATERIALIZED VIEW synthetix.view_trades AS
WITH 
rates AS (
    SELECT currency_key, currency_rate, evt_block_time
    FROM synthetix."ExchangeRates_evt_RatesUpdated" r, unnest("currencyKeys", "newRates") as u(currency_key, currency_rate)
)
SELECT
    trade.evt_block_time AS block_time,
    'Synthetix' AS project,
    '1' AS version,
    trade.account AS trader_a,
    NULL::bytea AS trader_b,
    trade."fromAmount"/1e18 AS token_a_amount_raw,
    trade."toAmount"/1e18 AS token_b_amount_raw,
    (SELECT address FROM synthetix.view_synths WHERE trade."fromCurrencyKey" = currency_key AND evt_block_time < trade.evt_block_time ORDER BY evt_block_time DESC LIMIT 1) AS token_a_address,        
    (SELECT address FROM synthetix.view_synths WHERE trade."toCurrencyKey" = currency_key AND evt_block_time < trade.evt_block_time ORDER BY evt_block_time DESC LIMIT 1) AS token_b_address,
    trade.contract_address AS exchange_contract_address,
    trade.evt_tx_hash AS tx_hash,
    NULL::integer[] AS trace_address,
    trade.evt_index AS evt_index,
    trade."fromAmount"/1e18 * (SELECT currency_rate FROM rates WHERE trade."fromCurrencyKey" = currency_key AND evt_block_time < trade.evt_block_time ORDER BY evt_block_time DESC LIMIT 1)/1e18 as token_a_amount,
    trade."toAmount"/1e18 * (SELECT currency_rate FROM rates WHERE trade."toCurrencyKey" = currency_key AND evt_block_time < trade.evt_block_time ORDER BY evt_block_time DESC LIMIT 1)/1e18 as token_b_amount
FROM
    synthetix."Synthetix_evt_SynthExchange" trade;

CREATE UNIQUE INDEX IF NOT EXISTS view_trades_id ON synthetix.view_trades (block_time,evt_index);
CREATE INDEX view_trades_block_time ON synthetix.view_trades (block_time);
CREATE INDEX view_trades_token_a_address ON synthetix.view_trades (token_a_address);
CREATE INDEX view_trades_token_b_address ON synthetix.view_trades (token_b_address);
CREATE INDEX view_trades_token_a_amount ON synthetix.view_trades (token_a_amount);

SELECT cron.schedule('0,30 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY synthetix.view_trades');
COMMIT;
