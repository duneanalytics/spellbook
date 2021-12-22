BEGIN;
CREATE TEMP TABLE existing_trades as (
    SELECT *
    FROM gnosis_protocol_v2.view_trades
);
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol_v2.view_trades;

CREATE MATERIALIZED VIEW gnosis_protocol_v2.view_trades AS
-- 1. Fetch the trades over the past day and populate with most recent prices, token details and unit amounts
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
                                 AND s.minute > NOW() - interval '1 day'
                                 AND s.minute = date_trunc('minute', evt_block_time)
             LEFT OUTER JOIN prices.usd as b
                             ON b.contract_address = (
                                 CASE
                                     WHEN trades."buyToken" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                                         THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                     ELSE trades."buyToken"
                                     END)
                                 AND s.minute > NOW() - interval '1 day'
                                 AND b.minute = date_trunc('minute', evt_block_time)
    WHERE evt_block_time > NOW() - interval '1 day'
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
-- 2. Construct a bijection between all recent order_uids <-> app hash
     batches_with_nested_uids_and_appdata AS
         (SELECT call_tx_hash,
                 array_agg("orderUid" ORDER BY evt_index ASC)                                  as uids,
                 (select jsonb_agg(t -> 'appData') from jsonb_array_elements(trades) as x(t))  as app_data,
                 (select jsonb_agg(t -> 'receiver') from jsonb_array_elements(trades) as x(t)) as receiver
          FROM gnosis_protocol_v2."GPv2Settlement_call_settle"
                   JOIN gnosis_protocol_v2."GPv2Settlement_evt_Trade"
                        ON call_tx_hash = evt_tx_hash
          WHERE evt_block_time > NOW() - interval '1 day'
          GROUP BY call_tx_hash, trades
         ),

     uid_to_app_id AS (
         SELECT unnest(uids)                                        as uid,
                trim('"' from jsonb_array_elements(app_data)::text) as app_data,
                trim('"' from jsonb_array_elements(receiver)::text) as receiver
         FROM batches_with_nested_uids_and_appdata
     ),

     deduplicated_app_uid_map as (
         select distinct on (uid) uid, app_data, receiver
         from uid_to_app_id
     ),
-- 3. Assemble the trades view for the past day according to the schema in existing_trades
     valued_trades as (
         SELECT block_time,
                tx_hash,
                order_uid,
                owner                                           as trader,
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
                    END)                                        as trade_value_usd,
                buy_price * units_bought                        as buy_value_usd,
                sell_price * units_sold                         as sell_value_usd,
                fee,
                fee_atoms,
                (CASE
                     WHEN sell_price IS NOT NULL THEN
                         CASE
                             WHEN buy_price IS NOT NULL and buy_price * units_bought > sell_price * units_sold
                                 then buy_price * units_bought * fee / units_sold
                             ELSE sell_price * fee
                             END
                     WHEN sell_price IS NULL AND buy_price IS NOT NULL THEN buy_price * units_bought * fee / units_sold
                     ELSE NULL::numeric
                    END)                                        as fee_usd,
                app_data,
                CONCAT('\x', substring(receiver from 3))::bytea as receiver
         FROM trades_with_token_units
                  JOIN deduplicated_app_uid_map
                       ON uid = order_uid
         ORDER BY block_time DESC
     ),
-- 4. By taking the union of the trades just assembled with the existing trades for the same time period
--    we eliminate identical duplicates, but will be left with records where one trade now contains a buy or sell price
--    that wasn't available before
     possible_overlap as (
         select *
         from existing_trades
         where block_time > NOW() - INTERVAL '1 day'
         union
         select *
         from valued_trades
     ),

-- 5. Eliminate trade occurring multiple times by taking distinct records for each
--    order_uid and transaction hash, ordered by buy_value_usd, sell_value_usd (both descending with nulls last).
--    This ensures the record with the most filled entries to be selected
     unique_new_entries as (
         select distinct on (order_uid, tx_hash) order_uid,
                                                 block_time,
                                                 tx_hash,
                                                 trader,
                                                 sell_token_address,
                                                 sell_token,
                                                 buy_token_address,
                                                 buy_token,
                                                 units_sold,
                                                 atoms_sold,
                                                 units_bought,
                                                 atoms_bought,
                                                 trade_value_usd,
                                                 buy_value_usd,
                                                 sell_value_usd,
                                                 fee,
                                                 fee_atoms,
                                                 fee_usd,
                                                 app_data,
                                                 receiver
         FROM possible_overlap
         ORDER BY order_uid, tx_hash, trade_value_usd desc
     ),

-- 6. The following preliminary results contain all the most updated trade information regarding prices.
--    However, because the erc20.tokens table is updated infrequently (and manually), it can occur that trades older
--    than the '1 day' specified as our cutoff would fall behind our update window and be forever without token details.
-- In what follows, we query these complete "preliminary results" for rows with unknown token data,
-- namely NULL values for units_bought OR units_sold - because these can't be populated without knowing token decimals.
     preliminary_results as (
         select *
         from (
                  select block_time,
                         tx_hash,
                         order_uid,
                         trader,
                         sell_token_address,
                         sell_token,
                         buy_token_address,
                         buy_token,
                         units_sold,
                         atoms_sold,
                         units_bought,
                         atoms_bought,
                         trade_value_usd,
                         buy_value_usd,
                         sell_value_usd,
                         fee,
                         fee_atoms,
                         fee_usd,
                         app_data,
                         receiver
                  from unique_new_entries
                  union
                  select *
                  from existing_trades
                  where block_time <= NOW() - INTERVAL '1 day'
              ) as _
     ),

     missing_token_trades as (
         SELECT *
         FROM preliminary_results
         WHERE units_bought IS NULL
            OR units_sold IS NULL
     ),

     backfilled_trade_info as (
         SELECT app_data,
                atoms_bought,
                atoms_sold,
                block_time,
                (CASE
                     WHEN b.symbol IS NULL THEN TEXT(buy_token)
                     ELSE b.symbol
                    END)                           as buy_token,
                buy_token_address,
                fee_atoms / 10 ^ s.decimals        as fee,
                fee_atoms,
                order_uid,
                receiver,
                (CASE
                     WHEN s.symbol IS NULL THEN TEXT(sell_token)
                     ELSE s.symbol
                    END)                           as sell_token,
                sell_token_address,
                trader,
                tx_hash,
                atoms_bought / pow(10, b.decimals) as units_bought,
                atoms_sold / pow(10, s.decimals)   as units_sold
         FROM missing_token_trades
                  LEFT OUTER JOIN erc20.tokens s
                                  ON sell_token_address = s.contract_address
                  LEFT OUTER JOIN erc20.tokens b
                                  ON buy_token_address = b.contract_address
     ),

     missing_valued_trades as (
         SELECT b.*,
                pb.price * units_bought as buy_value_usd,
                ps.price * units_sold   as sell_value_usd,
                (CASE
                     WHEN ps.price IS NOT NULL THEN
                         -- Choose the larger of two prices when both not null.
                         CASE
                             WHEN pb.price IS NOT NULL and pb.price * units_bought > ps.price * units_sold
                                 then pb.price * units_bought
                             ELSE ps.price * units_sold
                             END
                     WHEN ps.price IS NULL AND pb.price IS NOT NULL THEN pb.price * units_bought
                     ELSE NULL::numeric
                    END)                as trade_value_usd,
                (CASE
                     WHEN ps.price IS NOT NULL THEN
                         CASE
                             WHEN pb.price IS NOT NULL and pb.price * units_bought > ps.price * units_sold
                                 then pb.price * units_bought * fee / units_sold
                             ELSE ps.price * fee
                             END
                     WHEN ps.price IS NULL AND pb.price IS NOT NULL THEN pb.price * units_bought * fee / units_sold
                     ELSE NULL::numeric
                    END)                as fee_usd
         FROM backfilled_trade_info b
                  LEFT OUTER JOIN prices.usd ps
                                  ON ps.contract_address = sell_token_address
                                      AND ps.minute = date_trunc('minute', block_time)
                  LEFT OUTER JOIN prices.usd pb
                                  ON pb.contract_address = (
                                      CASE
                                          WHEN buy_token_address = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                                              THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                          ELSE buy_token_address
                                          END)
                                      AND pb.minute = date_trunc('minute', block_time)
     ),

     results as (
         SELECT *
         FROM preliminary_results
         WHERE units_bought IS NOT NULL
           AND units_sold IS NOT NULL
         UNION
         select block_time,
                tx_hash,
                order_uid,
                trader,
                sell_token_address,
                sell_token,
                buy_token_address,
                buy_token,
                units_sold,
                atoms_sold,
                units_bought,
                atoms_bought,
                trade_value_usd,
                buy_value_usd,
                sell_value_usd,
                fee,
                fee_atoms,
                fee_usd,
                app_data,
                receiver
         from missing_valued_trades
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
FROM results
ORDER BY block_time DESC;

CREATE UNIQUE INDEX IF NOT EXISTS view_trades_id ON gnosis_protocol_v2.view_trades (order_uid, tx_hash);
CREATE INDEX view_trades_idx_1 ON gnosis_protocol_v2.view_trades (block_time);
CREATE INDEX view_trades_idx_2 ON gnosis_protocol_v2.view_trades (sell_token_address);
CREATE INDEX view_trades_idx_3 ON gnosis_protocol_v2.view_trades (buy_token_address);
CREATE INDEX view_trades_idx_4 ON gnosis_protocol_v2.view_trades (trader);
CREATE INDEX view_trades_idx_5 ON gnosis_protocol_v2.view_trades (app_data);
CREATE INDEX view_trades_idx_6 ON gnosis_protocol_v2.view_trades (tx_hash);


INSERT INTO cron.job (schedule, command)
VALUES ('*/5 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol_v2.view_trades')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
