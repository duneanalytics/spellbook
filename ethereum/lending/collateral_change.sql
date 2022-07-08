CREATE TABLE IF NOT EXISTS lending.collateral_change (
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


CREATE OR REPLACE FUNCTION lending.insert_collateral_changes(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH collateral_change AS (
    SELECT
        project,
        version,
        collateral.block_number,
        collateral.block_time,
        tx_hash,
        evt_index,
        trace_address,
        tx."from" AS tx_from,
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
        WHERE evt_block_time >= start_ts
        AND evt_block_time < end_ts

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
        WHERE evt_block_time >= start_ts
        AND evt_block_time < end_ts

        UNION ALL
        -- Aave 2 add collateral
        SELECT
            'Aave' AS project,
            '2' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            "user" AS borrower,
            reserve AS asset_address,
            amount AS asset_amount
        FROM aave_v2."LendingPool_evt_Deposit"
        WHERE evt_block_time >= start_ts
        AND evt_block_time < end_ts

        UNION ALL
        -- Aave 2 remove collateral
        SELECT
            'Aave' AS project,
            '2' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            "user" AS borrower,
            reserve AS asset_address,
            -amount AS asset_amount
        FROM aave_v2."LendingPool_evt_Withdraw"
        WHERE evt_block_time >= start_ts
        AND evt_block_time < end_ts

        UNION ALL
        --Aave 2 liquidation calls
        SELECT
            'Aave' AS project,
            '2' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            "user" AS borrower,
            "collateralAsset" AS asset_address,
            -"liquidatedCollateralAmount" AS asset_amount
        FROM aave_v2."LendingPool_evt_LiquidationCall"
        WHERE evt_block_time >= start_ts
        AND evt_block_time < end_ts
        AND "receiveAToken" = FALSE

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
            SELECT * FROM compound_v2."cErc20_evt_Mint" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
            UNION ALL
            SELECT * FROM compound_v2."cEther_evt_Mint" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
            UNION ALL
            SELECT * FROM compound_v2."CErc20Delegator_evt_Mint" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
        ) compound_add
        LEFT JOIN compound.view_ctokens c ON compound_add.contract_address = c.contract_address

        UNION ALL
        -- Compound remove collateral
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
            SELECT * FROM compound_v2."cErc20_evt_Redeem" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
            UNION ALL
            SELECT * FROM compound_v2."cEther_evt_Redeem" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
            UNION ALL
            SELECT * FROM compound_v2."CErc20Delegator_evt_Redeem" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
        ) compound_redeem
        LEFT JOIN compound.view_ctokens c ON compound_redeem.contract_address = c.contract_address

        UNION ALL
        -- IronBank add collateral
        SELECT
            'IronBank' AS project,
            '1' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            minter AS borrower,
            i."underlying_token_address" AS asset_address,
            "mintAmount" AS asset_amount
        FROM (
            SELECT * FROM ironbank."CErc20Delegator_evt_Mint" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
        ) ironbank_add
        LEFT JOIN ironbank.view_itokens i ON ironbank_add.contract_address = i.contract_address

        UNION ALL
        -- IronBank remove collateral
        SELECT
            'IronBank' AS project,
            '1' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            redeemer AS borrower,
            i."underlying_token_address" AS asset_address,
            -"redeemAmount" AS asset_amount
        FROM (
            SELECT * FROM ironbank."CErc20Delegator_evt_Redeem" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
        ) ironbank_redeem
        LEFT JOIN ironbank.view_itokens i ON ironbank_redeem.contract_address = i.contract_address

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
        AND evt_block_time >= start_ts
        AND evt_block_time < end_ts

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
        AND evt_block_time >= start_ts
        AND evt_block_time < end_ts
    ) collateral
    INNER JOIN ethereum.transactions tx
        ON collateral.tx_hash = tx.hash
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
    LEFT JOIN erc20.tokens t ON t.contract_address = collateral.asset_address
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', collateral.block_time) AND p.contract_address = collateral.asset_address AND p.minute >= start_ts AND p.minute < end_ts
),
rows AS (
    INSERT INTO lending.collateral_change (
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
    FROM collateral_change
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;


CREATE UNIQUE INDEX IF NOT EXISTS lending_collateral_change_tr_addr_uniq_idx ON lending.collateral_change (tx_hash, trace_address);
CREATE UNIQUE INDEX IF NOT EXISTS lending_collateral_change_evt_index_uniq_idx ON lending.collateral_change (tx_hash, evt_index);
CREATE INDEX IF NOT EXISTS lending_collateral_change_block_time_idx ON lending.collateral_change USING BRIN (block_time);

SELECT lending.insert_collateral_changes('2019-01-01', (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-01-01'), (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')) WHERE NOT EXISTS (SELECT * FROM lending.collateral_change LIMIT 1);
INSERT INTO cron.job (schedule, command)
VALUES ('14 0 * * *', $$SELECT lending.insert_collateral_changes((SELECT max(block_time) - interval '2 days' FROM lending.collateral_change), (SELECT now() - interval '20 minutes'), (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '2 days' FROM lending.collateral_change)), (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
