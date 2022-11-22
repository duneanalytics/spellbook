CREATE TABLE IF NOT EXISTS lending.liquidation (
    project text NOT NULL,
    version text,
    block_time timestamptz NOT NULL,
    block_number numeric NOT NULL,
    tx_hash bytea,
    evt_index integer,
    trace_address integer[],
    liquidated_borrower bytea,
    tx_from bytea,
    debt_to_cover_asset_address bytea,
    collateral_asset_address bytea,
    debt_to_cover_asset_symbol text,
    debt_to_cover_token_amount numeric,
    debt_to_cover_usd_value numeric
);


CREATE OR REPLACE FUNCTION lending.insert_liquidation(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH liquidation AS (
    SELECT
        project,
        version,
        liquidation.block_number,
        liquidation.block_time,
        tx_hash,
        evt_index,
        trace_address,
        tx."from" AS tx_from,
        liquidated_borrower,
        t.symbol AS debt_to_cover_asset_symbol,
        debt_to_cover_asset_address,
        collateral_asset_address,
        debt_to_cover_amount / 10^t.decimals AS debt_to_cover_token_amount,
        debt_to_cover_amount / 10^t.decimals*p.price AS debt_to_cover_usd_value
    FROM (

        -- Aave liquidation
        SELECT
            'Aave' AS project,
            '1' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            "_user" AS liquidated_borrower,
            CASE --Use WETH instead of Aave "mock" address
                WHEN _reserve = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE _reserve
            END AS debt_to_cover_asset_address,
            CASE --Use WETH instead of Aave "mock" address
                WHEN _collateral = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE _collateral
            END AS collateral_asset_address,
            "_purchaseAmount" AS debt_to_cover_amount
        FROM aave."LendingPool_evt_LiquidationCall"
        WHERE evt_block_time >= start_ts
        AND evt_block_time < end_ts

        UNION ALL
        --Aave 2 liquidation
        SELECT
            'Aave' AS project,
            '2' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            "user" AS liquidated_borrower,
            "debtAsset" AS debt_to_cover_asset_address,
            "collateralAsset" AS collateral_asset_address,
            "debtToCover" AS debt_to_cover_amount
        FROM aave_v2."LendingPool_evt_LiquidationCall"
        WHERE evt_block_time >= start_ts
        AND evt_block_time < end_ts
        AND "receiveAToken" = FALSE

    UNION ALL
    -- Compound liquidation

    SELECT
        'Compound' AS project,
        '2' AS version,
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        evt_tx_hash AS tx_hash,
        evt_index,
        NULL::integer[] AS trace_address,
        borrower AS liquidated_borrower,
        c_asset."asset_underlying_token_address" AS debt_to_cover_asset_address,
        c_collateral."collateral_underlying_token_address" AS collateral_asset_address,
        "repayAmount" AS debt_to_cover_amount
    FROM (
        SELECT * FROM compound_v2."cErc20_evt_LiquidateBorrow" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
        UNION ALL
        SELECT * FROM compound_v2."cEther_evt_LiquidateBorrow" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
        UNION ALL
        SELECT * FROM compound_v2."CErc20Delegator_evt_LiquidateBorrow" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
    ) c
    LEFT JOIN (SELECT contract_address as collateral_contract_address, 
                        underlying_token_address as collateral_underlying_token_address
                FROM compound.view_ctokens) c_collateral ON c."cTokenCollateral" = c_collateral.collateral_contract_address
    LEFT JOIN (SELECT contract_address as asset_contract_address, 
                        underlying_token_address as asset_underlying_token_address
                FROM compound.view_ctokens) c_asset ON c.contract_address = c_asset.asset_contract_address

        UNION ALL
    -- IronBank liquidation

    SELECT
        'IronBank' AS project,
        '1' AS version,
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        evt_tx_hash AS tx_hash,
        evt_index,
        NULL::integer[] AS trace_address,
        borrower AS liquidated_borrower,
        i_asset."asset_underlying_token_address" AS debt_to_cover_asset_address,
        i_collateral."collateral_underlying_token_address" AS collateral_asset_address,
        "repayAmount" AS debt_to_cover_amount
    FROM (
        SELECT * FROM ironbank."CErc20Delegator_evt_LiquidateBorrow" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
    ) i
    LEFT JOIN (SELECT contract_address as collateral_contract_address, 
                        underlying_token_address as collateral_underlying_token_address
                FROM ironbank.view_itokens) i_collateral ON i."cTokenCollateral" = i_collateral.collateral_contract_address
    LEFT JOIN (SELECT contract_address as asset_contract_address, 
                        underlying_token_address as asset_underlying_token_address
                FROM ironbank.view_itokens) i_asset ON i.contract_address = i_asset.asset_contract_address

        UNION ALL
        -- MakerDAO liquidation
        SELECT
            'MakerDAO' AS project,
            '2' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            liquidated_borrower,
            '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea AS debt_to_cover_asset_address,
            m."underlying_token_address" AS collateral_asset_address,
            debt_to_cover_amount
        FROM (
            -- Liquidation V1.0
            SELECT evt_block_number, evt_block_time, evt_tx_hash, 
                   "flip" AS contract_address, evt_index,
                   "art" AS debt_to_cover_amount, "urn" AS liquidated_borrower 
            FROM makermcd."CAT_evt_Bite"
            WHERE art > 0
            AND evt_block_time >= start_ts
            AND evt_block_time < end_ts

            UNION ALL
            -- Liquidation V2.0 
            SELECT evt_block_number, evt_block_time, evt_tx_hash, 
                   "clip" AS contract_address, evt_index,
                   "art" AS debt_to_cover_amount, "urn" AS liquidated_borrower
            FROM makerdao."Dog_evt_Bark"
            WHERE art > 0
            AND evt_block_time >= start_ts
            AND evt_block_time < end_ts
        ) maker_liq
        LEFT JOIN (SELECT * FROM makermcd.flipper_addresses
                   UNION ALL
                   SELECT * FROM makermcd.clipper_addresses) m 
        ON maker_liq.contract_address = m.contract_address

    ) liquidation
    INNER JOIN ethereum.transactions tx
        ON liquidation.tx_hash = tx.hash
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
    LEFT JOIN erc20.tokens t ON t.contract_address = liquidation.debt_to_cover_asset_address
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', liquidation.block_time) AND p.contract_address = liquidation.debt_to_cover_asset_address AND p.minute >= start_ts AND p.minute < end_ts
    
),
rows AS (
    INSERT INTO lending.liquidation (
       project,
       version,
       block_time,
       block_number,
       tx_hash,
       evt_index,
       trace_address,
       tx_from,
       liquidated_borrower,
       debt_to_cover_asset_address,
       collateral_asset_address,
       debt_to_cover_asset_symbol,
       debt_to_cover_token_amount,
       debt_to_cover_usd_value
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
       liquidated_borrower,
       debt_to_cover_asset_address,
       collateral_asset_address,
       debt_to_cover_asset_symbol,
       debt_to_cover_token_amount,
       debt_to_cover_usd_value
    FROM liquidation
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;


CREATE UNIQUE INDEX IF NOT EXISTS lending_liquidation_tr_addr_uniq_idx ON lending.liquidation (tx_hash, trace_address);
CREATE UNIQUE INDEX IF NOT EXISTS lending_liquidation_evt_index_uniq_idx ON lending.liquidation (tx_hash, evt_index);
CREATE INDEX IF NOT EXISTS lending_liquidation_block_time_idx ON lending.liquidation USING BRIN (block_time);

SELECT lending.insert_liquidation('2019-01-01', (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-01-01'), (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')) WHERE NOT EXISTS (SELECT * FROM lending.liquidation LIMIT 1);
INSERT INTO cron.job (schedule, command)
VALUES ('14 0 * * *', $$SELECT lending.insert_liquidation((SELECT max(block_time) - interval '2 days' FROM lending.liquidation), (SELECT now() - interval '20 minutes'), (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '2 days' FROM lending.liquidation)), (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
