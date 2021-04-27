-- The prices.layer1_usd_btc table is no longer present but some queries depend on it.
CREATE OR REPLACE VIEW prices.layer1_usd_btc AS
select *
from prices.layer1_btc
where symbol = 'BTC';