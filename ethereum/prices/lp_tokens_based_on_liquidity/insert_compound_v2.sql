CREATE OR REPLACE FUNCTION prices.insert_compound_v2(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH hours as (
    SELECT hour FROM generate_series(start_ts, date_trunc('hour', end_ts::TIMESTAMP), '1 hour') g(hour)
),

prices AS (
    SELECT
        DATE_TRUNC('hour', minute) AS hour,
        contract_address,
        decimals,
        AVG(price) AS price
    FROM prices."usd"
    WHERE minute >= start_ts AND minute < end_ts
    AND contract_address IN ( SELECT underlying_token_address FROM compound."view_ctokens" )
    GROUP BY 1,2,3
),

mintRedeem_tx AS (
    SELECT
        date_trunc('hour', evt_block_time) AS hour,
        contract_address,
        "mintAmount" AS amount,
        "mintTokens" AS ctoken
    FROM compound_v2."cErc20_evt_Mint"
    WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
    UNION ALL
    SELECT
        date_trunc('hour', evt_block_time) AS hour,
        contract_address,
        "mintAmount" AS amount,
        "mintTokens" AS ctoken
    FROM compound_v2."cEther_evt_Mint"
    WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
    UNION ALL
    SELECT
        date_trunc('hour', evt_block_time) AS hour,
        contract_address,
        "mintAmount" AS amount,
        "mintTokens" AS ctoken
    FROM compound_v2."CErc20Delegator_evt_Mint"
    WHERE evt_block_time >= start_ts AND evt_block_time < end_ts

    UNION ALL

    SELECT
        date_trunc('hour', evt_block_time) AS hour,
        contract_address,
        "redeemAmount" AS amount,
        "redeemTokens" AS ctoken
    FROM compound_v2."cErc20_evt_Redeem"
    WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
    UNION ALL
    SELECT
        date_trunc('hour', evt_block_time) AS hour,
        contract_address,
        "redeemAmount" AS amount,
        "redeemTokens" AS ctoken
    FROM compound_v2."cEther_evt_Redeem"
    WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
    UNION ALL
    SELECT
        date_trunc('hour', evt_block_time) AS hour,
        contract_address,
        "redeemAmount" AS amount,
        "redeemTokens" AS ctoken
    FROM compound_v2."CErc20Delegator_evt_Redeem"
    WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
),

mintRedeem AS (
    SELECT
        hour,
        contract_address,
        SUM(amount) AS amount,
        SUM(ctoken) AS cToken
    FROM mintRedeem_tx
    GROUP BY 1,2
),

price_with_gap AS (
    SELECT
        mr.hour,
        LEAD(mr.hour, 1, now()) OVER (PARTITION BY mr.contract_address ORDER BY mr.hour) AS next_hour,
        mr.contract_address,
        (mr.amount/10^p.decimals)*p.price/NULLIF((mr.cToken/10^t.decimals),0) AS price,
        t.symbol,
        t.decimals
    FROM mintRedeem mr
    LEFT JOIN compound."view_ctokens" t USING (contract_address)
    LEFT JOIN prices p ON p.contract_address = t.underlying_token_address AND mr.hour = p.hour
),

rows AS (
    INSERT INTO prices.lp_tokens_based_on_liquidity (
        hour,
        contract_address,
        price,
        symbol,
        decimals,
        project,
        version
    )

    SELECT
        h.hour,
        p.contract_address,
        p.price,
        p.symbol,
        p.decimals,
        'Compound' AS project,
        '2' AS version
    FROM price_with_gap p
    INNER JOIN hours h ON p.hour <= h.hour AND h.hour < p.next_hour

    ON CONFLICT DO NOTHING
    RETURNING 1
)

SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Compound V2 contracts data stats on '2019-05-07'
-- yearly fill with overlap (so lead function is not null)

--filling 2019 and 2020
SELECT prices.insert_compound_v2(
    '2019-05-07 00:00',
    '2021-02-01 00:00'
)
WHERE NOT EXISTS (
    SELECT *
    FROM prices.lp_tokens_based_on_liquidity
    WHERE hour >= '2019-05-07 00:00'
    AND hour < '2021-02-01 00:00'
    AND project = 'Compound'
    AND version = '2'
);

--filling 2021
SELECT prices.insert_compound_v2(
    '2021-01-01 00:00',
    '2022-03-04 00:00'
)
WHERE NOT EXISTS (
    SELECT *
    FROM prices.lp_tokens_based_on_liquidity
    WHERE hour >= '2021-02-01 00:00'
    AND hour < '2022-03-04 00:00'
    AND project = 'Compound'
    AND version = '2'
)
;

-- fill 2022 and following
SELECT prices.insert_compound_v2(
    '2022-01-01 00:00',
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM prices.lp_tokens_based_on_liquidity
    WHERE hour >= '2022-01-01'
    AND hour < now() - interval '20 minutes'
    AND project = 'Compound'
    AND version = '2'
);

INSERT INTO cron.job (schedule, command)
VALUES ('17 3 * * *', $$
    SELECT prices.insert_compound_v2(
        (SELECT max(hour) FROM prices.lp_tokens_based_on_liquidity WHERE project = 'Compound' and version = '2'),
        (SELECT now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;