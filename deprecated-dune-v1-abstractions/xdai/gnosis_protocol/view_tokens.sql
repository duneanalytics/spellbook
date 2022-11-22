BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol.view_tokens CASCADE;
CREATE MATERIALIZED VIEW gnosis_protocol.view_tokens AS

SELECT CAST(id AS INT) AS token_id,
       tokens.contract_address AS token,
       symbol,
       decimals,
       evt_block_time AS add_date
FROM erc20.tokens AS tokens
INNER JOIN gnosis_protocol."BatchExchange_evt_TokenListing" AS listing
ON tokens.contract_address = listing.token;

CREATE UNIQUE INDEX IF NOT EXISTS view_tokens_id ON gnosis_protocol.view_tokens (token_id);
CREATE INDEX view_tokens_1 ON gnosis_protocol.view_tokens (symbol);
CREATE INDEX view_tokens_2 ON gnosis_protocol.view_tokens (token);

-- INSERT INTO cron.job (schedule, command)
-- VALUES ('*/5 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol.view_tokens')
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
-- COMMIT;
