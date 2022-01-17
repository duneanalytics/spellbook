BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol_v2.view_trades;

CREATE MATERIALIZED VIEW gnosis_protocol_v2.view_trades AS
-- Since there is no reliable price feed, we can only assign price of 1 to stable coins.
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
           (CASE
                WHEN trades."sellToken" IN (
                                            '\xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d',
                                            '\xDDAfbb505ad214D7b80b1f830fcCc89B60fb7A83',
                                            '\x4ECaBa5870353805a9F068101A40E0f32ed605C6',
                                            '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
                                            '\x44fa8e6f47987339850636f88629646662444217',
                                            '\x0905ab807f8fd040255f0cf8fa14756c1d824931'
                    ) THEN 1.0
                ELSE NULL
               END)                     as sell_price,
           (CASE
                WHEN trades."buyToken" IN (
                                           '\xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d',
                                           '\xDDAfbb505ad214D7b80b1f830fcCc89B60fb7A83',
                                           '\x4ECaBa5870353805a9F068101A40E0f32ed605C6',
                                           '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
                                           '\x44fa8e6f47987339850636f88629646662444217',
                                           '\x0905ab807f8fd040255f0cf8fa14756c1d824931'
                    ) THEN 1.0
                ELSE NULL
               END)                     as buy_price
    FROM gnosis_protocol_v2."GPv2Settlement_evt_Trade" trades
             LEFT OUTER JOIN prices.usd as s
                             ON trades."sellToken" = s.contract_address
                                 AND s.minute > TO_DATE('2021/03/03', 'YYYY/MM/DD') --! Deployment Date
                                 AND s.minute = date_trunc('minute', evt_block_time)
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
                owner                    as trader,
                sell_token_address,
                sell_token,
                buy_token_address,
                buy_token,
                units_sold,
                atoms_sold,
                units_bought,
                atoms_bought,
                (CASE
                     WHEN sell_price IS NOT NULL THEN
                         -- Choose the larger of two prices when both not null.
                         CASE
                             WHEN buy_price IS NOT NULL and buy_price * units_bought > sell_price * units_sold
                                 then buy_price * units_bought
                             ELSE sell_price * units_sold
                             END
                     WHEN sell_price IS NULL AND buy_price IS NOT NULL THEN buy_price * units_bought
                     ELSE NULL::numeric
                    END)                 as trade_value_usd,
                buy_price * units_bought as buy_value_usd,
                sell_price * units_sold  as sell_value_usd,
                fee,
                fee_atoms,
                (CASE
                     WHEN sell_price IS NOT NULL THEN
                         CASE
                             -- Units sold is sometimes zero we add this
                             WHEN units_sold = 0
                                 THEN 0
                             WHEN buy_price IS NOT NULL AND buy_price * units_bought > sell_price * units_sold
                                 THEN buy_price * units_bought * fee / units_sold
                             ELSE sell_price * fee
                             END
                     WHEN sell_price IS NULL AND buy_price IS NOT NULL
                         THEN buy_price * units_bought * fee / units_sold
                     ELSE NULL::numeric
                    END)                 as fee_usd
         FROM trades_with_token_units
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

CREATE UNIQUE INDEX IF NOT EXISTS view_trades_id ON gnosis_protocol_v2.view_trades (order_uid, tx_hash);
CREATE INDEX view_trades_idx_1 ON gnosis_protocol_v2.view_trades (block_time);
CREATE INDEX view_trades_idx_2 ON gnosis_protocol_v2.view_trades (sell_token_address);
CREATE INDEX view_trades_idx_3 ON gnosis_protocol_v2.view_trades (buy_token_address);
CREATE INDEX view_trades_idx_4 ON gnosis_protocol_v2.view_trades (trader);
CREATE INDEX view_trades_idx_6 ON gnosis_protocol_v2.view_trades (tx_hash);


INSERT INTO cron.job (schedule, command)
VALUES ('*/30 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol_v2.view_trades')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;