BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol.view_movement;
CREATE MATERIALIZED VIEW gnosis_protocol.view_movement AS
WITH
deposits AS (
    SELECT
        'deposit' AS operation,
        "batchId" AS batch_id,
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
        amount,
        RANK() OVER (PARTITION BY trader, token ORDER BY batch_id) AS rank
    FROM (
        SELECT
            'withdraw-request' AS operation,
            "batchId" AS batch_id,
            "user" AS trader,
            token,
            -amount AS amount,
            RANK() OVER (
                PARTITION BY "batchId", "user", token
                ORDER BY evt_block_number desc, evt_index desc
           ) AS withdraw_sub_id -- Within the same batch/token/use, new requests override the previous one
        FROM gnosis_protocol."BatchExchange_evt_WithdrawRequest" withdraw
        WHERE
            "batchId" < (floor(extract(epoch from now()) / 300)::INTEGER) -- discard future withdrawals
    ) w WHERE withdraw_sub_id = 1
),
withdraw AS (
    -- emited both, on a new request, or in an actual withdraw
    SELECT
        operation,
        batch_id,
        trader,
        token,
        SUM(amount) AS amount, -- although it's strange, there could be multiple withdraws in a batch (with amount 0), we don't duplicated registries
        RANK() OVER (PARTITION BY trader, token ORDER BY batch_id) AS rank
    FROM (
        SELECT
            'withdraw' AS operation,
            (floor(extract(epoch from evt_block_time) / 300)::INTEGER) AS batch_id,
            "user" AS trader,
            token,
            -amount AS amount
        FROM gnosis_protocol."BatchExchange_evt_Withdraw" withdraw
    ) w
    GROUP BY
        operation,
        batch_id,
        trader,
        token
),
actual_withdraws AS (
    SELECT
        withdraw.operation,
        withdraw.batch_id,
        withdraw.trader,
        withdraw.token,
        withdraw.amount,
        withdraw_request.batch_id AS batch_id_request,
        withdraw_request.amount AS pending_withdraw
    FROM withdraw
    JOIN withdraw_request
        ON withdraw.trader = withdraw_request.trader
        AND withdraw.token = withdraw_request.token
        AND withdraw.rank = withdraw_request.rank
),
sell AS (
    SELECT
        'sell' AS operation,
        batch_id,
        "trader_hex" AS trader,
        sell_token AS token,
        -sell_amount_atoms AS amount
    FROM gnosis_protocol.view_trades
    WHERE revert_time is NULL
),
buy AS (
    SELECT
        'buy' AS operation,
        batch_id,
        "trader_hex" AS trader,
        buy_token AS token,
        buy_amount_atoms AS amount
    FROM gnosis_protocol.view_trades
    WHERE revert_time is NULL
),
rewards AS (
    SELECT
        'solver-reward' AS operation,
        batch_id,
        submitter AS trader,
        decode('0905ab807f8fd040255f0cf8fa14756c1d824931', 'hex') AS token, -- OWL
        amount -- received owl
    FROM (
        SELECT
            FLOOR(EXTRACT(epoch from evt_block_time) / 300) - 1 AS batch_id,
            "burntFees" AS amount, -- OWL
            submitter,
            evt_block_number AS block_number,
            evt_index,
            RANK() OVER(
                PARTITION BY FLOOR(EXTRACT(epoch from evt_block_time) / 300) - 1
                ORDER BY evt_block_number DESC, evt_index DESC
            ) AS rank
        FROM gnosis_protocol."BatchExchange_evt_SolutionSubmission"
    ) s
    WHERE rank = 1
),
operations AS (
    -- Amounts:
    --      amount_deposited: Takes only add operations, actual withdraws, and trades (but not the requests)
    --      amount:           Amount considered for balance, can be negative, includes the "withdraw request" but not the actual withdraw
    -- BASic Add operartions:
    SELECT operation, batch_id, trader, token, amount AS amount_deposited, amount AS amount FROM deposits
    UNION SELECT operation, batch_id, trader, token, amount, amount FROM buy
    UNION SELECT operation, batch_id, trader, token, amount, amount FROM rewards
    -- BASic Substract operation:
    UNION SELECT operation, batch_id, trader, token, 0, amount FROM withdraw_request
    UNION SELECT operation, batch_id, trader, token, amount, amount FROM sell
    -- Special CASe Withdraw:
    --      Handle a withdraw AS a counter-movement of the request.
    --      It can be seen AS a counter movement of the difference between what wAS withdrawn and what wAS requested
    UNION SELECT
        operation,
        batch_id,
        trader,
        token,
        amount AS amount_deposited, -- Deduct the amount from the deposited amount
        amount - pending_withdraw AS amount -- Revert the discounted amount from the request, update with the actual
    FROM actual_withdraws
),
operation_details AS (
    SELECT
        operations.operation,
        operations.batch_id,
        operations.trader,
        operations.token,
        operations.amount_deposited,
        operations.amount,
        token.token_id,
        COALESCE(token.symbol, 'TOKEN-' || token.token_id) AS token_symbol,
        COALESCE(token.decimals, 18) AS decimals
    FROM operations
    JOIN gnosis_protocol.view_tokens token
        ON token.token = operations.token
),
balances AS (
    SELECT
        b.*,
        SUM(amount_deposited_atoms) OVER (
            PARTITION BY trader, token
            ORDER BY batch_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS balance_deposited_atoms,
        SUM(amount_atoms) OVER (
            PARTITION BY trader, token
            ORDER BY batch_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS balance_atoms
    FROM (
        SELECT
            batch_id,
            trader,
            string_agg(operation, ', ') AS operations,
            SUM(amount_deposited) AS amount_deposited_atoms,
            SUM(amount) AS amount_atoms,
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
    TO_TIMESTAMP((batch_id + 1) * 300) AS movement_date,
    batch_id,
    trader,
    operations,
    balances.amount_atoms/10^(balances.decimals) AS amount,
    amount_atoms,
    token_symbol,
    token,
    decimals,
    -- Balance: Available balance from user perspective (cannot be negative, and accounts for locked balance)
    CASE
        WHEN balances.balance_atoms > 0 THEN balances.balance_atoms/10^(balances.decimals)
        ELSE 0
    END AS balance,
    -- Actual balance: Can be negative
    balances.balance_atoms/10^(balances.decimals) AS balance_actual,
    balance_atoms AS balance_actual_atoms,
    -- Balance deposited: Balance in the contract (can be available or not)
    balances.balance_deposited_atoms/10^(balances.decimals) AS balance_deposited,
    balances.balance_deposited_atoms
FROM balances;


CREATE UNIQUE INDEX IF NOT EXISTS view_movement_id ON gnosis_protocol.view_movement (trader, batch_id, token) ;
CREATE INDEX view_movement_1 ON gnosis_protocol.view_movement (token_symbol);
CREATE INDEX view_movement_2 ON gnosis_protocol.view_movement (token);
CREATE INDEX view_movement_3 ON gnosis_protocol.view_movement (batch_id);

-- INSERT INTO cron.job (schedule, command)
-- VALUES ('*/5 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol.view_movement')
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
-- COMMIT;
