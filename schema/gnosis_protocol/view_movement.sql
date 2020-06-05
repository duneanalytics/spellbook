BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol.view_movement;
CREATE MATERIALIZED VIEW gnosis_protocol.view_movement AS
WITH
deposits as (
    SELECT
        'deposit' as operation,
        "batchId" + 1 as batch_id, -- batch id when it's credited
        "user" AS trader,
        token,
        amount
    FROM gnosis_protocol."BatchExchange_evt_Deposit" deposit
),
withdraw_request AS (
    SELECT 
        operation,
        batch_id,
        trader,
        token,
        amount
    FROM (
        SELECT
            'withdraw-request' as operation,
            "batchId" + 1 as batch_id, -- batch id when it's credited
            "user" as trader,
            token,
            -amount as amount,
            RANK() OVER (
                PARTITION BY "batchId", "user", token
                ORDER BY evt_block_number desc, evt_index desc
           ) as withdraw_sub_id -- Within the same batch/token/use, new requests override the previous one
        FROM gnosis_protocol."BatchExchange_evt_WithdrawRequest" withdraw
        WHERE 
            "batchId" < (floor(extract(epoch from now()) / 300)::INTEGER) -- discard future withdrawals
    ) w WHERE withdraw_sub_id = 1
),
sell AS (
    SELECT
        'sell' as operation,
        batch_id, -- sells are available right away
        "trader_hex" as trader,
        sell_token as token,
        -sell_amount_atoms as amount
    FROM gnosis_protocol."view_trades"
    WHERE revert_time is NULL
),
buy AS (
    SELECT
        'buy' as operation,
        batch_id, -- sells are available right away
        "trader_hex" as trader,
        buy_token as token,
        buy_amount_atoms as amount
    FROM gnosis_protocol."view_trades"
    WHERE revert_time is NULL
),
operations AS (
    SELECT * FROM deposits
    UNION SELECT * FROM withdraw_request
    UNION SELECT * FROM buy
    UNION SELECT * FROM sell
),
operation_details AS (
    SELECT
        operations.operation,
        operations.batch_id,
        operations.trader,
        operations.token,
        operations.amount,
        token.token_id,
        token.symbol as token_symbol,
        token.decimals
    FROM operations
    JOIN gnosis_protocol."view_tokens" token
        ON token.token = operations.token
),
balances AS (
    SELECT
        b.*,
        SUM(amount_atoms) OVER (
            PARTITION BY trader, token
            ORDER BY batch_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as balance_atoms
    FROM (
        SELECT
            batch_id,
            trader,
            string_agg(operation, ', ') as operations,
            SUM(amount) as amount_atoms,
            token_symbol,
            token,
            decimals
        FROM operation_details
        GROUP BY
            batch_id,
            trader,
            token,
            token_symbol,
            decimals
    ) b
)
SELECT 
    balances.*,
    balances.amount_atoms/10^(balances.decimals) as amount,
    balances.balance_atoms/10^(balances.decimals) as balance
FROM balances;


CREATE UNIQUE INDEX IF NOT EXISTS view_movement_id ON gnosis_protocol.view_movement (trader, batch_id, token) ;
CREATE INDEX view_movement_1 ON gnosis_protocol.view_movement (token_symbol);
CREATE INDEX view_movement_2 ON gnosis_protocol.view_movement (token);
CREATE INDEX view_movement_3 ON gnosis_protocol.view_movement (batch_id);

SELECT cron.schedule('0,5,10,15,20,25,30,35,40,45,50,55 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol.view_movement', NULL);
COMMIT;
