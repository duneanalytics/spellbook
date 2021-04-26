-- The prices.layer1_usd_eth table is no longer present but some queries depend on it.
CREATE OR REPLACE VIEW prices.layer1_usd_eth AS
select *
from prices.layer1_usd
where symbol = 'ETH';