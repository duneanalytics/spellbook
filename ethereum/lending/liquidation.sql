CREATE TABLE IF NOT EXISTS lending.liquidation (
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
    collateral_address bytea,
    asset_symbol text,
    collateral_token_amount numeric,
    usd_value numeric
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
        borrower,
        t.symbol AS asset_symbol,
        asset_address,
        collateral_address,
        collateral_amount / 10^t.decimals AS collateral_token_amount,
        collateral_amount / 10^t.decimals*p.price AS usd_value
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
            "_user" AS borrower,
            CASE --Use WETH instead of Aave "mock" address
                WHEN _reserve = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE _reserve
            END AS asset_address,
            CASE --Use WETH instead of Aave "mock" address
                WHEN _collateral = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE _collateral
            END AS collateral_address,
            "_liquidatedCollateralAmount" AS collateral_amount
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
            "user" AS borrower,
            "debtAsset" AS asset_address,
            "collateralAsset" AS collateral_address,
            "liquidatedCollateralAmount" AS collateral_amount
        FROM aave_v2."LendingPool_evt_LiquidationCall"
        WHERE evt_block_time >= start_ts
        AND evt_block_time < end_ts
        AND "receiveAToken" = FALSE

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
            borrower,
            '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea AS asset_address,
            m."underlying_token_address" AS collateral_address,
            collateral_amount
        FROM (
            -- Liquidation V1.0
            SELECT evt_block_number, evt_block_time, evt_tx_hash, contract_address, evt_index,
                   "lot" AS collateral_amount, "usr" AS borrower 
            FROM makermcd."FLIP_evt_Kick"
            WHERE tab > 0
            AND evt_block_time >= start_ts
            AND evt_block_time < end_ts

            UNION ALL
            -- Liquidation V2.0 
            SELECT evt_block_number, evt_block_time, evt_tx_hash, contract_address, evt_index,
                   "lot" AS collateral_amount, "usr" AS borrower
            FROM makerdao."Clipper_evt_Kick"
            WHERE tab > 0
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
    LEFT JOIN erc20.tokens t ON t.contract_address = liquidation.collateral_address
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', liquidation.block_time) AND p.contract_address = liquidation.collateral_address AND p.minute >= start_ts AND p.minute < end_ts
    
    UNION ALL
    -- Compound liquidation
    SELECT
        project,
        version,
        compound_liq.block_number,
        compound_liq.block_time,
        tx_hash,
        evt_index,
        trace_address,
        tx."from" AS tx_from,
        borrower,
        t2.symbol AS asset_symbol,
        asset_address,
        collateral_address,
        collateral_amount / 10^8 AS collateral_token_amount,
        collateral_amount / 10^8*dex_p.median_price AS usd_value
    FROM
        (SELECT
            'Compound' AS project,
            '2' AS version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index,
            NULL::integer[] AS trace_address,
            borrower AS borrower,
            c_asset."asset_underlying_token_address" AS asset_address,
            c_collateral."collateral_underlying_token_address" AS collateral_address,
            CASE --Use WBTC2 instead of WBTC2 cToken for price correctness
                WHEN c."cTokenCollateral" = '\xc11b1268c1a384e55c48c2391d8d480264a3a7f4' THEN '\xccF4429DB6322D5C611ee964527D42E5d685DD6a'
                ELSE c."cTokenCollateral"
            END AS c_token_collateral,
            "seizeTokens" AS collateral_amount
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
                   FROM compound.view_ctokens) c_asset ON c.contract_address = c_asset.asset_contract_address) compound_liq
        INNER JOIN ethereum.transactions tx
            ON compound_liq.tx_hash = tx.hash
            AND tx.block_number >= start_block
            AND tx.block_number < end_block
            AND tx.block_time >= start_ts
            AND tx.block_time < end_ts
        LEFT JOIN erc20.tokens t2 ON t2.contract_address = compound_liq.collateral_address
        LEFT JOIN prices."prices_from_dex_data" dex_p ON dex_p.hour = date_trunc('hour', compound_liq.block_time) AND 
                                                    dex_p.contract_address = compound_liq.c_token_collateral  AND
                                                    dex_p.hour >= start_ts AND  dex_p.hour < end_ts
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
       borrower,
       asset_address,
       asset_symbol,
       collateral_token_amount,
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


CREATE UNIQUE INDEX IF NOT EXISTS lending_liquidation_tr_addr_uniq_idx ON lending.liquidation (tx_hash, trace_address);
CREATE UNIQUE INDEX IF NOT EXISTS lending_liquidation_evt_index_uniq_idx ON lending.liquidation (tx_hash, evt_index);
CREATE INDEX IF NOT EXISTS lending_liquidation_block_time_idx ON lending.liquidation USING BRIN (block_time);

SELECT lending.insert_liquidation('2019-01-01', (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-01-01'), (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')) WHERE NOT EXISTS (SELECT * FROM lending.liquidation LIMIT 1);
INSERT INTO cron.job (schedule, command)
VALUES ('14 0 * * *', $$SELECT lending.insert_liquidation((SELECT max(block_time) - interval '2 days' FROM lending.liquidation), (SELECT now() - interval '20 minutes'), (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '2 days' FROM lending.liquidation)), (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
