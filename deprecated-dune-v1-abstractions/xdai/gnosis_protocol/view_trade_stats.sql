BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol.view_trade_stats;
CREATE MATERIALIZED VIEW gnosis_protocol.view_trade_stats AS
WITH trades AS (
    SELECT
        trades.trade_date,
        trades.batch_id,
        trades.trader_hex AS trader,
        trades.order_id,
        trades.trade_sub_id,
        trades.sell_amount,
        trades.sell_token_symbol,
        trades.sell_token_id,
        trades.buy_amount,
        trades.buy_token_symbol,
        trades.buy_token_id,
        (trades.sell_amount * price.token_usd_price) / 2 AS usd_volume,
        tx_hash AS trade_tx_hash
    FROM gnosis_protocol.view_trades trades
    JOIN gnosis_protocol.view_price_batch price
        ON trades.batch_id = price.batch_id
        AND trades.sell_token = price.token
),
view_trade_stats AS (
    SELECT
        trades.*,
        "priceNumerator" AS order_price_numerator,
        "priceDenominator" AS order_price_denominator,
        tx.hash AS order_tx,
        data::bytea::text ~ 'dec0de\d{8}$' AS has_analytics,
        substring(data::bytea::text, 'dec0de(\d{8})$') AS analytics
    FROM trades
    JOIN gnosis_protocol."BatchExchange_evt_OrderPlacement" orders
        ON trades.trader = orders.owner
        AND trades.order_id = orders.index
    LEFT OUTER JOIN xdai.transactions tx
        ON orders.evt_tx_hash = tx.hash
),
decoded_analalytics AS (
    SELECT
        trade_date,
        batch_id,
        trader,
        order_id,
        trade_sub_id,
        regexp_matches( -- Regex matches applies a filter, this is why this regex cannot be done in view_trade_stats and needs to be LEFT OUTER joined
            analytics,
            '(\d{2})(\d{2})(\d{1})(\d{2})(\d{1})$'
        ) AS analytics
    FROM view_trade_stats
)
SELECT
    stats.*,
    decoded.analytics[1] AS app_id,
    decoded.analytics[2] AS provider,
    CASE
        WHEN decoded.analytics[3] = '0' THEN true
        WHEN decoded.analytics[3] = '1' THEN false
        ELSE NULL
    END AS is_desktop,
    decoded.analytics[4] AS browser,
    decoded.analytics[5] AS screen_size
FROM view_trade_stats stats
LEFT OUTER JOIN decoded_analalytics decoded
    ON stats.batch_id = decoded.batch_id
      AND stats.trader = decoded.trader
      AND stats.order_id = decoded.order_id
      AND stats.trade_sub_id = decoded.trade_sub_id;


CREATE UNIQUE INDEX IF NOT EXISTS view_trade_stats_id ON gnosis_protocol.view_trade_stats (batch_id, trader, order_id, trade_sub_id);
CREATE INDEX view_trade_stats_idx_1 ON gnosis_protocol.view_trade_stats (app_id);
CREATE INDEX view_trade_stats_idx_2 ON gnosis_protocol.view_trade_stats (trade_date);


-- INSERT INTO cron.job (schedule, command)
-- VALUES ('*/10 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol.view_trades')
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
-- COMMIT;
