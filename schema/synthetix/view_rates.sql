CREATE OR REPLACE VIEW synthetix.view_rates AS 
SELECT currency_key, currency_rate, evt_block_time
FROM synthetix."ExchangeRates_evt_RatesUpdated" r, unnest("currencyKeys", "newRates") as u(currency_key, currency_rate)

UNION 

VALUES
('\x7355534400000000000000000000000000000000000000000000000000000000'::bytea, 1000000000000000000::bigint,	'2019-03-11T22:17:52.000Z'::timestamptz)
;
