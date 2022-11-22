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
        -- Aave V1
        SELECT
            'Aave' AS project,
            '1' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] as trace_address,
            borrower,
            CASE
                WHEN _reserve = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
                ELSE _reserve
            END AS asset_address,
            _amount AS asset_amount
        FROM (
            --lending
            SELECT evt_block_number, evt_block_time, evt_tx_hash, evt_index, _reserve, _amount, "_user" AS borrower
            FROM aave."LendingPool_evt_Borrow"
            WHERE evt_block_time >= start_ts
            AND evt_block_time < end_ts

            UNION ALL

            SELECT evt_block_number, evt_block_time, evt_tx_hash, evt_index, _reserve, _amount, "_target" AS borrower
            FROM aave."LendingPool_evt_FlashLoan"
            WHERE evt_block_time >= start_ts
            AND evt_block_time < end_ts
        ) aave


        UNION ALL

        -- Aave V2
        SELECT
            'Aave' AS project,
            '2' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] as trace_address,
            borrower,
            reserve AS asset_address,
            amount AS asset_amount
        FROM (
            --lending
            SELECT evt_block_number, evt_block_time, evt_tx_hash, evt_index, reserve, amount, "user" AS borrower
            FROM aave_v2."LendingPool_evt_Borrow"
            WHERE evt_block_time >= start_ts
            AND evt_block_time < end_ts

            UNION ALL

            SELECT evt_block_number, evt_block_time, evt_tx_hash, evt_index, asset AS reserve, amount, target AS borrower
            FROM aave_v2."LendingPool_evt_FlashLoan"
            WHERE evt_block_time >= start_ts
            AND evt_block_time < end_ts
        ) aave_v2

        UNION ALL

        -- Compound
        SELECT
            'Compound' AS project,
            '2' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] as trace_address,
            borrower,
            c."underlying_token_address" AS asset_address,
            "borrowAmount" AS asset_amount
        FROM (
            SELECT * FROM compound_v2."cErc20_evt_Borrow" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
            UNION ALL
            SELECT * FROM compound_v2."cEther_evt_Borrow" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
            UNION ALL
            SELECT * FROM compound_v2."CErc20Delegator_evt_Borrow" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
        ) events
        LEFT JOIN compound.view_ctokens c ON events.contract_address = c.contract_address

        UNION ALL

        -- IronBank V1
        SELECT
            'IronBank' AS project,
            '1' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] as trace_address,
            borrower,
            i."underlying_token_address" AS asset_address,
            "borrowAmount" AS asset_amount
        FROM (
            SELECT * FROM ironbank."CErc20Delegator_evt_Borrow" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
        ) events
        LEFT JOIN ironbank.view_itokens i ON events.contract_address = i.contract_address

        UNION ALL

        -- MakerDAO
        SELECT
            'MakerDAO' AS project,
            '2' AS version,
            call_block_number AS block_number,
            call_block_time AS block_time,
            call_tx_hash AS tx_hash,
            NULL::integer as evt_index,
            call_trace_address as trace_address,
            borrower,
            '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea AS asset_address,
            asset_amount
        FROM (
            --ERC20 Dai
            SELECT call_block_number, call_block_time, call_tx_hash, call_trace_address, "wad" AS asset_amount, "usr" AS borrower
            FROM makermcd."DAI_call_mint"
            WHERE call_success
            AND wad > 0
            AND call_block_time >= start_ts
            AND call_block_time < end_ts

            UNION ALL
            -- Dai in DSR
            SELECT call_block_number, call_block_time, call_tx_hash, call_trace_address, "rad"/1e27 AS asset_amount, "src" AS borrower
            FROM makermcd."VAT_call_move"
            WHERE call_success
            AND dst = '\x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
            AND rad > 0
            AND call_block_time >= start_ts
            AND call_block_time < end_ts
        ) maker
    ) borrow
    INNER JOIN ethereum.transactions tx
        ON borrow.tx_hash = tx.hash
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
    LEFT JOIN erc20.tokens t ON t.contract_address = borrow.asset_address
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

SELECT lending.insert_borrow('2019-01-01', (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-01-01'), (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')) WHERE NOT EXISTS (SELECT * FROM lending.borrow LIMIT 1);
INSERT INTO cron.job (schedule, command)
VALUES ('14 1 * * *', $$SELECT lending.insert_borrow((SELECT max(block_time) - interval '2 days' FROM lending.borrow), (SELECT now() - interval '20 minutes'), (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '2 days' FROM lending.borrow)), (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
