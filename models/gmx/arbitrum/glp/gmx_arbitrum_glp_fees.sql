{{ config(
        alias = 'glp_fees',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['minute'],
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "gmx",
                                    \'["1chioku"]\') }}'
        )
}}

WITH minute AS  -- This CTE generates a series of minute values
    (
    SELECT explode(sequence(TIMESTAMP '2021-08-31 08:13', CURRENT_TIMESTAMP, INTERVAL 1 minute)) AS minute -- 2021-08-31 08:13 is the timestamp of the first vault transaction
    ) ,

/*
GLP fees accrued to the Fee GLP contract and can be claimed by invoking function_claim()
The Fee GLP contract can be found here: https://arbiscan.io/address/0x4e971a87900b931ff39d1aad67697f49835400b6
*/

fglp_balances AS -- This CTE returns the accuals of WETH tokens in the Fee GLP contract and their respective balances in WETH and USD in a designated minute
    (
    SELECT  -- This query calculatees the balance in USD of accrued fees ini the Fee GLP contract
        c.minute,
        c.balance AS balance_weth
    FROM
        (
        SELECT -- This subquery aggregates the cumulative balance of WETH tokens in the Fee GLP contract over the minute series
            b.minute,
            SUM(b.transfer_value) OVER (ORDER BY b.minute ASC) AS balance
        FROM
            (
            SELECT  -- This subquery aggregates all the inbound tranfers of WETH tokens to the Fee GLP contract in a designated minute
                a.minute,
                SUM(a.transfer_value) AS transfer_value
            FROM
                (
                SELECT -- This subquery truncates the block time to a minute and selects all the inbound tranfers of WETH tokens to the Fee GLP contract
                    date_trunc('minute', evt_block_time) AS minute,
                    ((value) / 1e18) AS transfer_value -- WETH 18dp
                FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
                WHERE `to` = '0x4e971a87900b931ff39d1aad67697f49835400b6' -- Fee GLP contract
                AND `contract_address` = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' -- WETH Arbitrum Smart Contract
                ) a
            GROUP BY a.minute
            ) b
        ) c
    )

SELECT
    a.minute AS minute,
    COALESCE(b.fees_weth,0) AS fees_weth, -- Removes null values
    last(c.weth_current_price, true) OVER (ORDER BY a.minute ASC) AS weth_current_price  -- extrapolation for differential is data table syncing
FROM minute a    
LEFT JOIN
    (
    SELECT
        minute,
        last(balance_weth, true) OVER (ORDER BY minute ASC) AS fees_weth
    FROM fglp_balances 
    ) b
    ON a.minute = b.minute
LEFT JOIN
    (
    SELECT
        minute,
        weth_current_price
    FROM {{ref('gmx_arbitrum_glp_components')}}
    ) c 
    ON a.minute = c.minute