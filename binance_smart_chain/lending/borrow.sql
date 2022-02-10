BEGIN;
CREATE SCHEMA IF NOT EXISTS lending;

CREATE TABLE IF NOT EXISTS lending.borrow (
    project text NOT NULL,
    version text,
    block_time timestamptz NOT NULL,
    block_number numeric NOT NULL,
    tx_hash bytea,
    evt_index integer,
    trace_address integer[],
    borrower bytea,
    tx_from bytea,
    asset_address bytea,
    asset_symbol text,
    token_amount numeric,
    usd_value numeric
);


CREATE OR REPLACE FUNCTION lending.insert_borrow(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH borrow AS (
    SELECT
        project,
        version,
        borrow.block_number,
        borrow.block_time,
        tx_hash,
        evt_index,
        trace_address,
        tx."from" as tx_from,
        borrower,
        t.symbol AS asset_symbol,
        asset_address,
        asset_amount / 10^t.decimals AS token_amount,
        asset_amount / 10^t.decimals*p.price AS usd_value
    FROM (
        -- Venus
        SELECT
            'Venus' AS project,
            '1' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] as trace_address,
            borrower,
            c."underlying_token_address" AS asset_address,
            "borrowAmount" AS asset_amount
        FROM (
            SELECT "evt_block_number","evt_block_time","evt_tx_hash","evt_index","borrower","borrowAmount","contract_address"
            FROM venus."VBNB_evt_Borrow" 
            WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
            
            UNION ALL
            SELECT "evt_block_number","evt_block_time","evt_tx_hash","evt_index","borrower","borrowAmount","contract_address"
            FROM venus."VBep20Delegate_evt_Borrow" 
            WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
            
            UNION ALL
            SELECT "evt_block_number","evt_block_time","evt_tx_hash","evt_index","minter","mintVAIAmount","contract_address"
            FROM venus."VAIController_evt_MintVAI"
            WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
        ) events
        LEFT JOIN venus.view_vtokens c ON events.contract_address = c.contract_address
    ) borrow
    INNER JOIN bsc."transactions" tx
        ON borrow.tx_hash = tx.hash
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
    LEFT JOIN bep20.tokens t ON t.contract_address = borrow.asset_address
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', borrow.block_time) AND p.contract_address = borrow.asset_address AND p.minute >= start_ts AND p.minute < end_ts
),
rows AS (
    INSERT INTO lending.borrow (
       project,
       version,
       block_time,
       block_number,
       tx_hash,
       evt_index,
       trace_address,
       tx_from,
       borrower,
       asset_address,
       asset_symbol,
       token_amount,
       usd_value
    )
    SELECT
       project,
       version,
       block_time,
       block_number,
       tx_hash,
       evt_index,
       trace_address,
       tx_from,
       borrower,
       asset_address,
       asset_symbol,
       token_amount,
       usd_value
    FROM borrow
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;


CREATE UNIQUE INDEX IF NOT EXISTS lending_borrow_tr_addr_uniq_idx ON lending.borrow (tx_hash, trace_address);
CREATE UNIQUE INDEX IF NOT EXISTS lending_borrow_evt_index_uniq_idx ON lending.borrow (tx_hash, evt_index);
CREATE INDEX IF NOT EXISTS lending_borrow_block_time_idx ON lending.borrow USING BRIN (block_time);

SELECT lending.insert_borrow('2021-01-01', (SELECT now()), (SELECT max(number) FROM bsc.blocks WHERE time < '2021-01-01'), (SELECT MAX(number) FROM bsc.blocks where time < now() - interval '20 minutes')) WHERE NOT EXISTS (SELECT * FROM lending.borrow LIMIT 1);
COMMIT;

INSERT INTO cron.job (schedule, command)
VALUES ('14 1 * * *', $$SELECT lending.insert_borrow((SELECT max(block_time) - interval '2 days' FROM lending.borrow), (SELECT now() - interval '20 minutes'), (SELECT max(number) FROM bsc.blocks WHERE time < (SELECT max(block_time) - interval '2 days' FROM lending.borrow)), (SELECT MAX(number) FROM bsc.blocks where time < now() - interval '20 minutes'));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
