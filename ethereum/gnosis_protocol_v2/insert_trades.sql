CREATE OR REPLACE FUNCTION gnosis_protocol_v2.insert_trades(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
    LANGUAGE plpgsql AS
$function$
DECLARE
    r integer;
BEGIN
    WITH rows AS (
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
                                         AND s.minute = date_trunc('minute', evt_block_time)
                     LEFT OUTER JOIN prices.usd as b
                                     ON b.contract_address = (
                                         CASE
                                             WHEN trades."buyToken" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                                                 THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                             ELSE trades."buyToken"
                                             END)
                                         AND b.minute = date_trunc('minute', evt_block_time)
            WHERE evt_block_time >= start_ts
              AND evt_block_time < end_ts
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

            batches_with_nested_uids_and_appdata AS
                (SELECT call_tx_hash,
                        array_agg("orderUid" ORDER BY evt_index ASC)                                  as uids,
                        (select jsonb_agg(t -> 'appData') from jsonb_array_elements(trades) as x(t))  as app_data,
                        (select jsonb_agg(t -> 'receiver') from jsonb_array_elements(trades) as x(t)) as receiver
                 FROM gnosis_protocol_v2."GPv2Settlement_call_settle"
                          JOIN gnosis_protocol_v2."GPv2Settlement_evt_Trade"
                               ON call_tx_hash = evt_tx_hash
                 WHERE evt_block_time >= start_ts
                   AND evt_block_time < end_ts
                 GROUP BY call_tx_hash, trades
                ),

            uid_to_app_id AS (
                SELECT unnest(uids)                                        as uid,
                       trim('"' from jsonb_array_elements(app_data)::text) as app_data,
                       trim('"' from jsonb_array_elements(receiver)::text) as receiver
                FROM batches_with_nested_uids_and_appdata
                ),

            deduplicated_app_uid_map as (
                select distinct on (uid) uid, app_data, receiver from uid_to_app_id
                ),

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
                       buy_price,
                       buy_price * units_bought                        as buy_value_usd,
                       sell_price,
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
                            WHEN sell_price IS NULL AND buy_price IS NOT NULL
                                THEN buy_price * units_bought * fee / units_sold
                            ELSE NULL::numeric
                           END)                                        as fee_usd,
                       app_data,
                       CONCAT('\x', substring(receiver from 3))::bytea as receiver
                FROM trades_with_token_units
                         JOIN deduplicated_app_uid_map
                              ON uid = order_uid
                )
            INSERT INTO gnosis_protocol_v2.trades
                (app_data, atoms_bought, atoms_sold, block_time, buy_price, buy_token_address, buy_token, buy_value_usd,
                 fee, fee_atoms, fee_usd, order_uid, receiver, sell_price, sell_token_address, sell_token,
                 sell_value_usd, trader, trade_value_usd, tx_hash, units_bought, units_sold)
                SELECT app_data,
                       atoms_bought,
                       atoms_sold,
                       block_time,
                       buy_price,
                       buy_token_address,
                       buy_token,
                       buy_value_usd,
                       fee,
                       fee_atoms,
                       fee_usd,
                       order_uid,
                       receiver,
                       sell_price,
                       sell_token_address,
                       sell_token,
                       sell_value_usd,
                       trader,
                       trade_value_usd,
                       tx_hash,
                       units_bought,
                       units_sold
                FROM valued_trades
                ORDER BY block_time DESC
                -- We avoid conflict by dropping the values before executing the insertion (cf. the cronjobs below)
                ON CONFLICT DO NOTHING
                -- If we wanted to keep this method entirely independent of the cron schedule below, then we might use the following code block to handle conflicts.
--                 ON CONFLICT (order_uid, tx_hash) DO UPDATE SET
--                     trade_value_usd = EXCLUDED.trade_value_usd,
--                     units_bought = EXCLUDED.units_bought,
--                     units_sold = EXCLUDED.units_sold,
--                     sell_price = EXCLUDED.sell_price,
--                     buy_price = EXCLUDED.buy_price,
--                     fee = EXCLUDED.fee,
--                     fee_usd = EXCLUDED.fee_usd
--                     buy_token = EXCLUDED.buy_token
--                     sell_token = EXCLUDED.sell_token
                RETURNING 1
    )

    SELECT count(*)
    INTO r
    FROM rows;
    RETURN r;
END
$function$;


-- fill 2021: This is only ever relevant 1 time.
SELECT gnosis_protocol_v2.insert_trades(
               '2021-03-03', --! Deployment date
               '2022-01-01'
           )
WHERE NOT EXISTS(
        SELECT *
        FROM gnosis_protocol_v2.trades
        WHERE block_time >= '2021-03-03'
          AND block_time < '2022-01-01'
    );

-- For the two cron jobs defined below,
-- one is intended to back fill lagging price feed (while also including most recent trades).
-- The second, less frequent job is meant to back fill missing token data that is manually
-- updated on an irregular schedule. A three month time window should suffice for manual token updates.

-- Every five minutes we go back 1 day and repopulate the values.
-- This captures new trades since the previous run, but also includes
-- previously non-existent price data (since the price feed is slightly behind)
INSERT INTO cron.job (schedule, command)
VALUES ('*/5 * * * *', $$
    BEGIN;
    DELETE FROM gnosis_protocol_v2.trades
        WHERE block_time >= (SELECT DATE_TRUNC('day', now()) - INTERVAL '1 days');
    SELECT gnosis_protocol_v2.insert_trades(
        (SELECT DATE_TRUNC('day', now()) - INTERVAL '1 days')
    );
    COMMIT;
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

-- Once per day we go back 3 months repopulate the values.
-- This is intended to back fill any new erc20 token data that may have been introduced.
-- While simultaneously updating all fields relying on token data. Specifically, these are:
-- buy_token, sell_token, (for the symbol) and
-- trade_value_usd, units_bought, units_sold, sell_price, buy_price, fee, fee_usd (requiring token decimals).
--
-- NOTE that we choose to run this job daily at 1 minute past midnight,
-- so not to compete with the every 5 minute job above
INSERT INTO cron.job (schedule, command)
VALUES ('1 0 * * *', $$
    BEGIN;
    DELETE FROM gnosis_protocol_v2.trades
        WHERE block_time >= (SELECT DATE_TRUNC('day', now()) - INTERVAL '3 months');
    SELECT gnosis_protocol_v2.insert_trades(
        (SELECT DATE_TRUNC('day', now()) - INTERVAL '3 months')
    );
    COMMIT;
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
