BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol.view_tokens;
CREATE MATERIALIZED VIEW gnosis_protocol.view_tokens AS

-- TODO - Replace all occurences of listed_tokens with erc20.tokens when available. 
listed_tokens as (
    SELECT * FROM (VALUES
        (decode('0905ab807f8fd040255f0cf8fa14756c1d824931', 'hex'), 'OWL', 18),
        (decode('6a023ccd1ff6f2045c3309768ead9e68f978f6e1', 'hex'), 'wETH', 18),
        (decode('4ecaba5870353805a9f068101a40e0f32ed605c6', 'hex'), 'USDT', 6),
        (decode('44fA8E6f47987339850636F88629646662444217', 'hex'), '???-MistakenlyAdded', 18),
        (decode('e91d153e0b41518a2ce8dd3d7944fa863463a97d', 'hex'), 'wXDAI', 18),
        (decode('b7d311e2eb55f2f68a9440da38e7989210b9a05e', 'hex'), 'STAKE', 18),
        (decode('9c58bacc331c9aa871afd802db6379a98e80cedb', 'hex'), 'GNO', 18),
        (decode('b1950fb2c9c0cbc8553578c67db52aa110a93393', 'hex'), 'sUSD', 18),
        (decode('ddafbb505ad214d7b80b1f830fccc89b60fb7a83', 'hex'), 'USDC', 6),
        (decode('8e5bbbb09ed1ebde8674cda39a0c169401db4252', 'hex'), 'wBTC', 8),
        (decode('6293268785399bed001cb68a8ee04d50da9c854d', 'hex'), 'CRC', 18)
    ) as t (contract_address, symbol, decimals)
),

tokens as (
    SELECT
        ROW_NUMBER() OVER (ORDER BY transactions.block_number, transactions.index) as token_id,
        tokens.token,
        erc20.symbol,
        erc20.decimals,
        transactions.block_time as add_date
    FROM gnosis_protocol."BatchExchange_call_addToken" tokens
    JOIN xdai."transactions" transactions
      ON transactions.hash=tokens.call_tx_hash
      AND transactions.success=true
    LEFT OUTER JOIN listed_tokens as erc20
      ON erc20.contract_address = tokens.token
    UNION all (
    SELECT
        0 as token_id,
        '\x0905ab807f8fd040255f0cf8fa14756c1d824931' as token_address,
        'OWL' as symbol,
        18 as decimals,
        '2020-09-11 12:54:30.000' as add_date
    )
)

SELECT * FROM tokens
ORDER BY token_id;

CREATE UNIQUE INDEX IF NOT EXISTS view_tokens_id ON gnosis_protocol.view_tokens (token_id) ;
CREATE INDEX view_tokens_1 ON gnosis_protocol.view_tokens (symbol);
CREATE INDEX view_tokens_2 ON gnosis_protocol.view_tokens (token);

INSERT INTO cron.job (schedule, command)
VALUES ('*/5 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol.view_tokens')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
