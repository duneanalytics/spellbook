BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol.view_tokens;
CREATE MATERIALIZED VIEW gnosis_protocol.view_tokens AS
WITH 
token_names as (
    SELECT * FROM (VALUES
        (14, 'aDAI', 18),
        (17, 'PAXG', 18)
    ) as t (token_id, symbol, decimals)
),
tokens as (
    SELECT
        ROW_NUMBER() OVER (ORDER BY transactions.block_number, transactions.index) as token_id,
        tokens.token,
        erc20.symbol,
        erc20.decimals,
        transactions.block_time as add_date
    FROM gnosis_protocol."BatchExchange_call_addToken" tokens
    JOIN ethereum."transactions" transactions
      ON transactions.hash=tokens.call_tx_hash
      AND transactions.success=true
    LEFT OUTER JOIN erc20."tokens" as erc20
      ON erc20.contract_address = tokens.token
)
SELECT
    tokens.token_id,
    tokens.token,
    COALESCE(tokens.symbol, token_names.symbol) as symbol,
    COALESCE(tokens.decimals, token_names.decimals) as decimals,
    tokens.add_date
FROM tokens
LEFT OUTER JOIN token_names
    ON tokens.token_id = token_names.token_id
UNION all (
    SELECT
        0 as token_id,
        '\x1a5f9352af8af974bfc03399e3767df6370d82e4' as token_address,
        'OWL' as symbol,
        18 as decimals,
        '2020-01-23 20:30:00.000' as add_date
);

CREATE UNIQUE INDEX IF NOT EXISTS view_tokens_id ON gnosis_protocol.view_tokens (token_id) ;
CREATE INDEX view_tokens_1 ON gnosis_protocol.view_tokens (symbol);
CREATE INDEX view_tokens_2 ON gnosis_protocol.view_tokens (token);

-- INSERT INTO cron.job (schedule, command)
-- VALUES ('*/5 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol.view_tokens')
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
-- COMMIT;
