BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol.view_trade_stats;
CREATE MATERIALIZED VIEW gnosis_protocol.view_trade_stats AS
WITH trades as (
    SELECT 
        trades.trade_date,
        trades.batch_id,
        trades.trader_hex as trader,
        trades.order_id,
        trades.trade_sub_id,
        trades.sell_amount,
        trades.sell_token_symbol,
        trades.sell_token_id,
        trades.buy_amount,
        trades.buy_token_symbol,
        trades.buy_token_id,
        trades.sell_amount * price.token_usd_price as usd_volume,
        tx_hash as trade_tx_hash
    FROM gnosis_protocol.view_trades trades
    JOIN gnosis_protocol.view_price_batch price
        ON trades.batch_id = price.batch_id
        AND trades.sell_token = price.token
    WHERE trades.revert_time IS NULL
)
SELECT
    trades.*,
    "priceNumerator" as order_price_numerator,
    "priceDenominator" as order_price_denominator,
    tx.hash as order_tx,
    data::bytea::text ~ 'dec0de\d{8}$' as is_web,
    substring(data::bytea::text, 'dec0de(\d{8})$') as analytics
FROM trades
JOIN gnosis_protocol."BatchExchange_evt_OrderPlacement" orders
    ON trades.trader = orders.owner
    AND trades.order_id = orders.index
LEFT OUTER JOIN ethereum.transactions tx
    ON orders.evt_tx_hash = tx.hash;


CREATE UNIQUE INDEX IF NOT EXISTS view_trade_stats_id ON gnosis_protocol.view_trade_stats (batch_id, trader_hex, order_id, trade_sub_id);

SELECT cron.schedule('00,10,20,30,40,50 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol.view_trades');
COMMIT;
