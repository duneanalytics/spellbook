BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol_v2.view_trades;

CREATE MATERIALIZED VIEW gnosis_protocol_v2.view_trades AS
WITH trades_with_prices AS (
    SELECT evt_block_time               as block_time,
           evt_tx_hash                  as tx_hash,
           owner,
           "orderUid"                   as order_uid,
           "sellToken"                  as sell_token,
           "buyToken"                   as buy_token,
           ("sellAmount" - "feeAmount") as sell_amount,
           "buyAmount"                  as buy_amount,
           "feeAmount"                  as fee_amount,
           s.price                      as sell_price,
           b.price                      as buy_price
    FROM gnosis_protocol_v2."GPv2Settlement_evt_Trade" trades
             LEFT OUTER JOIN prices.usd as s
                             ON trades."sellToken" = s.contract_address
                                 AND s.minute > TO_DATE('2021/03/03', 'YYYY/MM/DD') --! Deployment Date
                                 AND s.minute = date_trunc('minute', evt_block_time)
             LEFT OUTER JOIN prices.usd as b
                             ON b.contract_address = (
                                 CASE
                                     WHEN trades."buyToken" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                                         THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                     ELSE trades."buyToken"
                                     END)
                                 AND b.minute > TO_DATE('2021/03/03', 'YYYY/MM/DD') --! Deployment Date
                                 AND b.minute = date_trunc('minute', evt_block_time)
),

     trades_with_token_units as (
         SELECT block_time,
                tx_hash,
                order_uid,
                owner,
                sell_token                    as sell_token_address,
                (CASE
                     WHEN s.symbol IS NULL THEN TEXT(sell_token)
                     ELSE s.symbol
                    END)                      as sell_token,
                buy_token                     as buy_token_address,
                (CASE
                     WHEN b.symbol IS NULL THEN TEXT(buy_token)
                     ELSE b.symbol
                    END)                      as buy_token,
                sell_amount / 10 ^ s.decimals as units_sold,
                sell_amount                   as atoms_sold,
                buy_amount / 10 ^ b.decimals  as units_bought,
                buy_amount                    as atoms_bought,
                -- We use sell value when possible and buy value when not
                fee_amount / 10 ^ s.decimals  as fee,
                fee_amount                    as fee_atoms,
                sell_price,
                buy_price
         FROM trades_with_prices
                  LEFT OUTER JOIN erc20.tokens s
                                  ON s.contract_address = sell_token
                  LEFT OUTER JOIN erc20.tokens b
                                  ON b.contract_address = buy_token
     ),

     valued_trades as (
         SELECT block_time,
                tx_hash,
                order_uid,
                owner    as trader,
                sell_token_address,
                sell_token,
                buy_token_address,
                buy_token,
                units_sold,
                atoms_sold,
                units_bought,
                atoms_bought,
                -- We use sell value when possible and buy value when not
                -- We use sell value when possible and buy value when not
                (CASE
                     WHEN sell_price IS NOT NULL THEN
                         -- Choose the larger of two prices when both not null.
                         CASE
                             WHEN buy_price IS NOT NULL and buy_price > sell_price then buy_price * units_bought
                             ELSE sell_price * units_sold
                             END
                     WHEN sell_price IS NULL AND buy_price IS NOT NULL THEN buy_price * units_bought
                     ELSE -0.01
                    END) as trade_value_usd,
                fee,
                fee_atoms,
                (CASE
                     WHEN sell_price IS NOT NULL THEN
                         CASE
                             WHEN buy_price IS NOT NULL and buy_price > sell_price then buy_price * units_bought * fee / units_sold
                             ELSE sell_price * fee
                             END
                     WHEN sell_price IS NULL AND buy_price IS NOT NULL THEN buy_price * units_bought * fee / units_sold
                     ELSE -0.01
                    END) as fee_usd
         FROM trades_with_token_units
         ORDER BY block_time DESC
     )
-- This would be the kind of basic table we display when querying: It seems impractical to store the URL links
-- created from the hashes (trader, transaction and order id) so they have not been included here.
-- SELECT
--     block_time,
--     CONCAT('<a href="https://etherscan.io/address/', CONCAT('0x', ENCODE(trader, 'hex')), '" target="_blank">', CONCAT('0x', ENCODE(trader, 'hex')),  '</a>') as trader,
--     sell_token,
--     buy_token,
--     units_sold,
--     units_bought,
--     trade_value_usd,
--     fee,
--     fee_usd,
--     CONCAT('<a href="https://etherscan.io/tx/', CONCAT('0x', ENCODE(tx_hash, 'hex')), '" target="_blank">', CONCAT('0x', ENCODE(tx_hash, 'hex')),  '</a>') as transaction,
--     CONCAT('<a href="https://gnosis-protocol.io/orders/', CONCAT('0x', ENCODE(order_uid, 'hex')), '" target="_blank">', CONCAT('0x', ENCODE(order_uid, 'hex')),  '</a>') as order_uid
-- FROM valued_trades

SELECT *
FROM valued_trades
ORDER BY block_time DESC;


CREATE UNIQUE INDEX IF NOT EXISTS view_trades_id ON gnosis_protocol_v2.view_trades (order_uid);
CREATE INDEX view_trades_idx_1 ON gnosis_protocol_v2.view_trades (block_time);
CREATE INDEX view_trades_idx_2 ON gnosis_protocol_v2.view_trades (sell_token_address);
CREATE INDEX view_trades_idx_3 ON gnosis_protocol_v2.view_trades (buy_token_address);
CREATE INDEX view_trades_idx_4 ON gnosis_protocol_v2.view_trades (trader);


INSERT INTO cron.job (schedule, command)
VALUES ('*/30 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol_v2.view_trades')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
