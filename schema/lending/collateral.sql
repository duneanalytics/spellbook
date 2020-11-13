CREATE TABLE lending.collateral_event (
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


CREATE OR REPLACE FUNCTION lending.insert_collateral_events(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH collateral_event AS (
    SELECT
        project,
        version,
        collateral.block_number,
        collateral.block_time,
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

        -- Aave add collateral
        SELECT
            'Aave' AS project,
            '1' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            "_user" AS borrower,
            CASE --Use WETH instead of Aave "mock" address
                WHEN _reserve = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE _reserve
            END AS asset_address,
            _amount AS asset_amount
        FROM aave."LendingPool_evt_Deposit"

        UNION ALL

        -- Aave remove collateral
        SELECT
            'Aave' AS project,
            '1' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            "_user" AS borrower,
            CASE --Use WETH instead of Aave "mock" address
                WHEN _reserve = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE _reserve
            END AS asset_address,
            -"_amount" AS asset_amount
        FROM aave."LendingPool_evt_RedeemUnderlying"

        UNION ALL

        -- Compound add collateral
        SELECT
            'Compound' AS project,
            '2' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            minter AS borrower,
            c."underlying_token_address" AS asset_address,
            "mintAmount" AS asset_amount
        FROM (
            SELECT * FROM compound_v2."cErc20_evt_Mint"
            UNION ALL
            SELECT * FROM compound_v2."cEther_evt_Mint"
            UNION ALL
            SELECT * FROM compound_v2."CErc20Delegator_evt_Mint"
        ) compound_add
        LEFT JOIN compound.view_ctokens c ON compound_add.contract_address = c.contract_address

        UNION ALL

        SELECT
            'Compound' AS project,
            '2' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            redeemer AS borrower,
            c."underlying_token_address" AS asset_address,
            -"redeemAmount" AS asset_amount
        FROM (
            SELECT * FROM compound_v2."cErc20_evt_Redeem"
            UNION ALL
            SELECT * FROM compound_v2."cEther_evt_Redeem"
            UNION ALL
            SELECT * FROM compound_v2."CErc20Delegator_evt_Redeem"
        ) compound_redeem
        LEFT JOIN compound.view_ctokens c ON compound_redeem.contract_address = c.contract_address

        UNION ALL

        -- MakerDAO add collateral
        SELECT
            'MakerDAO' AS project,
            '2' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            "from" AS borrower,
            tr.contract_address AS asset_address,
            value AS assset_amount
        FROM erc20."ERC20_evt_Transfer" tr
        WHERE "to" IN (SELECT address FROM makermcd.collateral_addresses)

        UNION ALL

        -- MakerDAO remove collateral
        SELECT
            'MakerDAO' AS project,
            '2' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            "to" AS borrower,
            tr.contract_address AS asset_address,
            -value AS assset_amount
        FROM erc20."ERC20_evt_Transfer" tr
        WHERE "from" IN (SELECT address FROM makermcd.collateral_addresses)
    ) collateral
    INNER JOIN ethereum.transactions tx ON collateral.tx_hash = tx.hash AND tx.block_number >= start_block AND tx.block_number < end_block
    LEFT JOIN erc20.tokens t ON t.contract_address = collateral.asset_address
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', collateral.block_time) AND p.contract_address = collateral.asset_address AND p.minute >= start_ts AND p.minute < end_ts
),
rows AS (
    INSERT INTO lending.collateral (
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
    FROM collateral_event
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;


CREATE UNIQUE INDEX IF NOT EXISTS lending_collateral_event_tr_addr_uniq_idx ON lending.collateral_event (tx_hash, trace_address, trade_id);
CREATE UNIQUE INDEX IF NOT EXISTS lending_collateral_event_evt_index_uniq_idx ON lending.collateral_event (tx_hash, evt_index, trade_id);
CREATE INDEX IF NOT EXISTS lending_collateral_event_block_time_idx ON lending.collateral_event USING BRIN (block_time);

INSERT INTO cron.job (schedule, command)
VALUES ('*/14 * * * *', $$SELECT lending.insert_collateral_events((SELECT max(block_time) - interval '1 days' FROM lending.collateral_event), (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM lending.collateral_event)), (SELECT MAX(number) FROM ethereum.blocks));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;