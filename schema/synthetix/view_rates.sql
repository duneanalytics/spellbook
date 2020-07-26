CREATE OR REPLACE VIEW synthetix.view_rates AS 
SELECT currency_key, currency_rate, evt_block_time
FROM synthetix."ExchangeRates_evt_RatesUpdated" r, unnest("currencyKeys", "newRates") as u(currency_key, currency_rate)
