-- The prices.layer1_usd_btc table is no longer present but some queries depend on it.
CREATE OR REPLACE VIEW prices.layer1_usd_btc AS
SELECT *
FROM prices.layer1_usd
WHERE symbol = 'BTC';

GRANT SELECT ON prices.layer1_usd_btc TO reader;
GRANT SELECT, INSERT, UPDATE ON prices.layer1_usd_btc TO dune;