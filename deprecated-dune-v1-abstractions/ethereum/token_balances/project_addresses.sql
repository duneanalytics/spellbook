CREATE TABLE IF NOT EXISTS token_balances.project_addresses (
    address	bytea PRIMARY KEY,
    project	text,
    details	text
);

CREATE OR REPLACE FUNCTION token_balances.insert_addresses() RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
    WITH rows AS (

        INSERT INTO token_balances.project_addresses (address, project, details)

        -- uniswap v1
        SELECT DISTINCT exchange AS address, 'Uniswap' AS project, 'v1' AS details
        FROM uniswap."Factory_evt_NewExchange"

        UNION ALL

        -- uniswap v2
        SELECT DISTINCT pair AS address, 'Uniswap' AS project, 'v2' AS details
        FROM uniswap_v2."Factory_evt_PairCreated"

        UNION ALL

        --compound v1
        SELECT DISTINCT contract_address AS address, 'Compound' AS project, 'v1' AS details
        FROM compound_v1."MoneyMarket_evt_SupplyReceived"

        UNION ALL
        --
        -- compound v2
        SELECT DISTINCT "cToken" AS address, 'Compound' AS project, 'v2' AS details
        FROM compound_v2."Unitroller_evt_MarketListed"

        UNION ALL

        -- maker multi collateral dai
        SELECT DISTINCT address, project, 'MCD' AS details
        FROM makermcd.collateral_addresses

        UNION ALL

        -- aave
        SELECT '\x3dfd23A6c5E8BbcFc9581d2E864a68feb6a076d3'::bytea AS address, 'Aave' AS project, NULL::text AS details
        --
        UNION ALL

        -- curve.fi
        SELECT DISTINCT(exchange_contract_address), 'Curve' AS project, NULL::text AS details
        FROM curvefi.view_trades

        ON CONFLICT (address) DO UPDATE SET project=EXCLUDED.project, details=EXCLUDED.details
        RETURNING 1
    )
    SELECT count(*) INTO r from rows;
    RETURN r;
END
$function$;

INSERT INTO cron.job (schedule, command)
VALUES ('59 * * * *', $$SELECT token_balances.insert_addresses();$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;