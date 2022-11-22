CREATE OR REPLACE VIEW zeroex.view_zrx_staking_changes AS (
WITH
    additions AS (
        SELECT
            HEX_TO_INT(RIGHT(mse."toPool"::VARCHAR,5)) AS pool_id
            , mse.staker
            , mse.evt_block_number AS block_number
            , tx.index AS transaction_index
            , amount / 1e18 AS amount
        FROM zeroex_v3."StakingProxy_evt_MoveStake" mse
        LEFT JOIN ethereum.transactions tx ON tx.hash = mse.evt_tx_hash
        WHERE
            -- to delegated
            mse."toStatus" = 1
    )
    , removals AS (
        SELECT
            HEX_TO_INT(RIGHT(mse."fromPool"::VARCHAR,5)) AS pool_id
            , staker
            , mse.evt_block_number AS block_number
            , tx.index AS transaction_index
            , -amount / 1e18 AS amount
        FROM zeroex_v3."StakingProxy_evt_MoveStake" mse
        LEFT JOIN ethereum.transactions tx ON tx.hash = mse.evt_tx_hash
        WHERE
            -- from delegated
            mse."fromStatus" = 1
    )
    SELECT * FROM additions
    
    UNION ALL
    
    SELECT * FROM removals
);
