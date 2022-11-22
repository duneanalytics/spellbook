CREATE OR REPLACE VIEW prices.prices_exclude_tokens AS

SELECT symbol FROM (
values
  ('TCAP'),('ArbiNYAN'), --prices skewed
  ('vAELIN'),('vKWENTA'), --underlying deal tokens - prices don't map well
  ('sETHo'), ('DHT'), ('ELK') --weirdly off
) a (symbol);
