{{
    config(
        schema = 'balancer_v2_ethereum',
        alias='bpt_prices',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["victorstefenon"]\') }}'
    )
}}

WITH bpt_trades AS (
    SELECT
        block_time,
        bpt_address,
        bpt_amount_raw,
        bpt_amount_raw / POWER(10, COALESCE(erc20a.decimals, 18)) AS bpt_amount,
        token_amount_raw,
        token_amount_raw / POWER(10, erc20b.decimals) AS token_amount,
        p.price * token_amount_raw / POWER(10, erc20b.decimals) AS usd_amount
    FROM (
        SELECT
            t.evt_block_time AS block_time,
            CASE
                WHEN t.tokenin = SUBSTRING(t.poolid, 0, 42) THEN t.tokenin
                ELSE t.tokenout
            END AS bpt_address,
            CASE
                WHEN t.tokenin = SUBSTRING(t.poolid, 0, 42) THEN t.amountin
                ELSE t.amountout
            END AS bpt_amount_raw,
            CASE
                WHEN t.tokenin = SUBSTRING(t.poolid, 0, 42) THEN t.tokenout
                ELSE t.tokenin
            END AS token_address,
            CASE
                WHEN t.tokenin = SUBSTRING(t.poolid, 0, 42) THEN t.amountout
                ELSE t.amountin
            END AS token_amount_raw
        FROM {{ source('balancer_v2_ethereum', 'Vault_evt_Swap') }} AS t
        WHERE
            t.tokenin = SUBSTRING(t.poolid, 0, 42)
            OR t.tokenout = SUBSTRING(t.poolid, 0, 42)
    ) AS dexs
    LEFT JOIN {{ ref('tokens_erc20') }} AS erc20a
        ON
            erc20a.contract_address = dexs.bpt_address
            AND erc20a.blockchain = "ethereum"
    INNER JOIN {{ ref('tokens_erc20') }} AS erc20b
        ON
            erc20b.contract_address = dexs.token_address
            AND erc20b.blockchain = "ethereum"
    LEFT JOIN {{ source('prices', 'usd') }} AS p ON
        p.minute = DATE_TRUNC("minute", dexs.block_time)
        AND p.contract_address = dexs.token_address AND p.blockchain = "ethereum"
),

bpt_estimated_prices AS (
    SELECT
        block_time,
        bpt_address,
        usd_amount / bpt_amount AS price
    FROM
        bpt_trades
)

SELECT
    DATE_TRUNC("hour", block_time) AS hour,
    bpt_address AS contract_address,
    PERCENTILE(price, 0.5) AS median_price
FROM bpt_estimated_prices
GROUP BY 1, 2
