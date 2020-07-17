CREATE OR REPLACE VIEW synthetix.view_trades (
    block_time,
    project,
    version,
    trader_a,
    trader_b,
    token_a_amount_raw,
    token_b_amount_raw,
    token_a_address,
    token_b_address,
    exchange_contract_address,
    tx_hash,
    trace_address,
    evt_index,
    token_a_amount,
    token_b_amount,
    token_a_usd_price,
    token_b_usd_price
) AS SELECT
    trade.evt_block_time AS block_time,
    'Synthetix' AS project,
    '1' AS version,
    trade.account AS trader_a,
    NULL::bytea AS trader_b,
    trade."fromAmount"/1e18 AS token_a_amount_raw,
    trade."toAmount"/1e18 AS token_b_amount_raw,
    synths_a.address AS token_a_address,
    synths_b.address AS token_b_address,
    trade.contract_address AS exchange_contract_address,
    trade.evt_tx_hash AS tx_hash,
    NULL::integer[] AS trace_address,
    trade.evt_index AS evt_index,
    trade."fromAmount"/1e18 * rates_a.currency_rate/1e18 as token_a_amount,
    trade."toAmount"/1e18 * rates_a.currency_rate/1e18 as token_b_amount,
    rates_a.currency_rate/1e18 as token_a_usd_price,
    rates_b.currency_rate/1e18 as token_b_usd_price
FROM
    synthetix."Synthetix_evt_SynthExchange" trade
LEFT JOIN synthetix."view_synths" synths_a
    ON trade."fromCurrencyKey" = synths_a.currency_key
    AND trade.evt_block_time >= synths_a.evt_block_time
    AND trade.evt_block_time < synths_a.max_block_time
LEFT JOIN synthetix."view_synths" synths_b
    ON trade."toCurrencyKey" = synths_b.currency_key
    AND trade.evt_block_time >= synths_b.evt_block_time
    AND trade.evt_block_time < synths_b.max_block_time
LEFT JOIN synthetix."view_synths_rates" rates_a
    ON trade."fromCurrencyKey" = rates_a.currency_key
    AND trade.evt_block_time >= rates_a.evt_block_time
    AND trade.evt_block_time < rates_a.max_block_time
LEFT JOIN synthetix."view_synths_rates" rates_b
    ON trade."toCurrencyKey" = rates_b.currency_key
    AND trade.evt_block_time >= rates_b.evt_block_time
    AND trade.evt_block_time < rates_b.max_block_time
;
