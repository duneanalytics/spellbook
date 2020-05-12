CREATE MATERIALIZED VIEW gnosis_protocol.view_trades AS
WITH reverts as (
SELECT
    reversion.owner,
    reversion."orderId" as order_id,
    reversion.evt_block_time as block_time,
    RANK() OVER (
        PARTITION BY solution."batchId", reversion.owner, reversion."orderId"
        ORDER BY reversion.evt_block_number, reversion.evt_index
    ) as trade_sub_id,
    solution."batchId" as batch_id
FROM gnosis_protocol."BatchExchange_evt_TradeReversion" reversion
JOIN gnosis_protocol."BatchExchange_call_submitSolution" solution
    ON solution.call_tx_hash=reversion.evt_tx_hash
    AND solution.call_success = true
),
trades as (
SELECT
	  -- id
    floor(extract(epoch from solution.evt_block_time) / 300) - 1 AS batch_id, -- The event time tells us the batch. Between minute 0-4 is resolved batch N-1
    trades."owner" AS trader_hex,    
    trades."orderId" as order_id,
    RANK() OVER (
        PARTITION BY solution.evt_block_time, trades.owner, trades."orderId"
        ORDER BY trades.evt_block_number, trades.evt_index
    ) as trade_sub_id,
    -- Event index
    trades.evt_index AS evt_index_trades,
    solution.evt_index AS evt_index_solution,
    -- dates & block info
    solution.evt_block_number,
    solution.evt_block_time as block_time,
    to_timestamp((floor(extract(epoch from solution.evt_block_time) / 300)) * 300) AS trade_date,
    -- sell token
    trades."sellToken" as sell_token_id,
    sell_token."token" as sell_token,
    sell_token."symbol" as sell_token_symbol,
    sell_token."decimals" as sell_token_decimals,
    -- sell amounts
    trades."executedSellAmount" as sell_amount_atoms,
    trades."executedSellAmount" / 10^(sell_token.decimals) as sell_amount,
    -- buy token
    trades."buyToken" as buy_token_id,
    buy_token.token as buy_token,    
    buy_token.symbol as buy_token_symbol,
    buy_token.decimals as buy_token_decimals,
    -- buy amounts
    trades."executedBuyAmount" as buy_amount_atoms,
    trades."executedBuyAmount" / 10^(buy_token.decimals) as buy_amount,
    -- Tx and block info
    trades.evt_block_number as block_number,
    trades.evt_tx_hash as tx_hash
FROM gnosis_protocol."BatchExchange_evt_Trade" trades
JOIN gnosis_protocol."BatchExchange_evt_SolutionSubmission" solution
    ON solution.evt_tx_hash=trades.evt_tx_hash
JOIN gnosis_protocol.view_tokens buy_token
    ON trades."buyToken" = buy_token.token_id
JOIN gnosis_protocol.view_tokens sell_token
    ON trades."sellToken" = sell_token.token_id    
)
SELECT
	trades.*,
	reverts.block_time as revert_time
FROM trades
LEFT OUTER JOIN reverts
    ON trades.trader_hex = reverts.owner
    AND trades.order_id = reverts.order_id
    AND trades.trade_sub_id = reverts.trade_sub_id
    AND trades.batch_id = reverts.batch_id
-- WHERE trades.batch_id = 5267142
ORDER BY 
    trades.batch_id,
    trades.trader_hex,
    trades.order_id,
    trade_sub_id;




CREATE UNIQUE INDEX IF NOT EXISTS view_trades_id ON gnosis_protocol.view_trades (batch_id, trader_hex, order_id, trade_sub_id);
CREATE INDEX view_trades_idx_1 ON gnosis_protocol.view_trades (trade_date);
CREATE INDEX view_trades_idx_2 ON gnosis_protocol.view_trades (sell_token_symbol);
CREATE INDEX view_trades_idx_3 ON gnosis_protocol.view_trades (sell_token);
CREATE INDEX view_trades_idx_4 ON gnosis_protocol.view_trades (buy_token_symbol);
CREATE INDEX view_trades_idx_5 ON gnosis_protocol.view_trades (buy_token);
CREATE INDEX view_trades_idx_6 ON gnosis_protocol.view_trades (trader_hex, order_id);



SELECT cron.schedule('0-59 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol.view_trades');
