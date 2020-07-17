BEGIN;
DROP MATERIALIZED VIEW IF EXISTS synthetix.view_synths_rates;
CREATE MATERIALIZED VIEW synthetix.view_synths_rates AS
WITH 
rates AS (
    SELECT currency_key, currency_rate, evt_block_time
    FROM synthetix."ExchangeRates_evt_RatesUpdated" r, unnest("currencyKeys", "newRates") as u(currency_key, currency_rate)
)
SELECT 
    a.currency_key,
    a.currency_rate,
    a.evt_block_time,
    b.evt_block_time as max_block_time
FROM rates a
LEFT JOIN LATERAL (
    SELECT evt_block_time
    FROM rates joined_rates
    WHERE a.currency_key = joined_rates.currency_key
    AND a.evt_block_time < joined_rates.evt_block_time
    ORDER BY joined_rates.evt_block_time ASC
    LIMIT 1
) b ON TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS view_synths_rates_id ON synthetix.view_synths_rates (currency_key,evt_block_time);
CREATE INDEX view_synths_rates_evt_block_time ON synthetix.view_synths_rates (evt_block_time);
CREATE INDEX view_synths_rates_max_block_time ON synthetix.view_synths_rates (max_block_time);
CREATE INDEX view_synths_rates_currency_key ON synthetix.view_synths_rates (currency_key);

SELECT cron.schedule('0,5,10,15,20,25,30,35,40,45,50,55 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY synthetix.view_synths_rates');
COMMIT;
