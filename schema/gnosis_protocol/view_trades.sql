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
),
trades as (
SELECT
    solution."batchId" as batch_id,
    trades.evt_block_time as block_time,
    trades."orderId" as order_id,
    trades."owner",
    trades.evt_block_number as block_number,
    trades.evt_index,
    trades."buyToken" as buy_token_id,
    trades."sellToken" as sell_token_id,
    trades."executedBuyAmount" as executed_buy_amount,
    trades."executedSellAmount" as executed_sell_amount,
    trades.evt_tx_hash as tx_hash,
    RANK() OVER (
        PARTITION BY solution."batchId", trades.owner, trades."orderId"
        ORDER BY trades.evt_block_number, trades.evt_index
    ) as trade_sub_id
FROM gnosis_protocol."BatchExchange_evt_Trade" trades
JOIN gnosis_protocol."BatchExchange_call_submitSolution" solution
    ON solution.call_tx_hash=trades.evt_tx_hash
)
SELECT
    trades.batch_id,
    trades.owner as trader_hex,
    trades.order_id,
    trades.block_time,
    trades.block_number,
    reverts.block_time as revert_time,
    sell_token."token" as sell_token,
    sell_token."token" as sell_token,
    sell_token."symbol" as sell_token_symbol,
    sell_token."decimals" as sell_token_decimals,
    trades.executed_sell_amount as sell_amount_atoms,
    trades.executed_sell_amount / 10^(sell_token.decimals) as sell_amount,
    buy_token.token as buy_token,
    buy_token.symbol as buy_token_symbol,
    buy_token.decimals as sell_token_decimals,
    trades.executed_buy_amount as buy_amount_atoms,
    trades.executed_buy_amount / 10^(buy_token.decimals) as buy_amount,
    ((trades.executed_buy_amount / 10^(buy_token.decimals)) / (trades.executed_sell_amount / 10^(sell_token.decimals))) as price,
    trades.tx_hash,
    CONCAT('https://etherscan.io/tx/','0x', ENCODE(trades.tx_hash, 'hex')) as solution_tx_link,    
    CONCAT('0x', ENCODE(trades."owner", 'hex')) as trader
FROM trades
LEFT OUTER JOIN reverts
    ON trades.owner = reverts.owner
    AND trades.order_id = reverts.order_id
    AND trades.trade_sub_id = reverts.trade_sub_id
    AND trades.batch_id = reverts.batch_id
JOIN gnosis_protocol.view_tokens buy_token
    ON trades.buy_token_id = buy_token.token_id
JOIN gnosis_protocol.view_tokens sell_token
    ON trades.sell_token_id = sell_token.token_id
ORDER BY 
    trades.batch_id,
    trades.owner,
    trades.order_id;


CREATE UNIQUE INDEX IF NOT EXISTS view_trades_id ON gnosis_protocol.view_trades (batch_id, trader_hex, order_id) ;
CREATE INDEX view_trades_1 ON gnosis_protocol.view_trades (batch_id);
CREATE INDEX view_trades_2 ON gnosis_protocol.view_trades (sell_token_symbol);
CREATE INDEX view_trades_3 ON gnosis_protocol.view_trades (sell_token);
CREATE INDEX view_trades_4 ON gnosis_protocol.view_trades (buy_token_symbol);
CREATE INDEX view_trades_5 ON gnosis_protocol.view_trades (buy_token);
CREATE INDEX view_trades_6 ON gnosis_protocol.view_trades (trader_hex);
CREATE INDEX view_trades_7 ON gnosis_protocol.view_trades (trader_hex, order_id);


SELECT cron.schedule('0/1 0 * * *', 'REFRESH MATERIALIZED VIEW gnosis_protocol.view_trades');
