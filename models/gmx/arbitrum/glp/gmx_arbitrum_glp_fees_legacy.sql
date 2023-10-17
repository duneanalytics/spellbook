{{ config(
	tags=['legacy'],
	
        alias = alias('glp_fees', legacy_model=True),
        partition_by = ['block_date'],
        file_format = 'delta',
        unique_key = ['block_date', 'minute'],
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "gmx",
                                    \'["1chioku"]\') }}'
        )
}}

{% set project_start_date = '2021-08-31 08:13' %}

WITH minute AS  -- This CTE generates a series of minute values
    (
    SELECT *
    FROM
        (
        SELECT explode(sequence(TIMESTAMP '{{project_start_date}}', CURRENT_TIMESTAMP, INTERVAL 1 minute)) AS minute -- 2021-08-31 08:13 is the timestamp of the first vault transaction
        )
    ),

/*
GLP fees accrued to the Fee GLP contract and can be claimed by invoking function_claim()
The Fee GLP contract can be found here: https://arbiscan.io/address/0x4e971a87900b931ff39d1aad67697f49835400b6
*/

fglp_balances AS -- This CTE returns the accuals of WETH tokens in the Fee GLP contract in a designated minute
    (    
    SELECT -- This subquery aggregates the cumulative balance of WETH tokens in the Fee GLP contract over the minute series
        b.minute,
        b.weth_transfer_value,
        SUM(b.weth_transfer_value) OVER (ORDER BY b.minute ASC) AS weth_cum_balance
    FROM
        (
        SELECT  -- This subquery aggregates all the inbound tranfers of WETH tokens to the Fee GLP contract in a designated minute
            a.minute,
            SUM(a.transfer_value) AS weth_transfer_value
        FROM
            (
            SELECT -- This subquery truncates the block time to a minute and selects all the inbound tranfers of WETH tokens to the Fee GLP contract
                date_trunc('minute', evt_block_time) AS minute,
                ((value) / 1e18) AS transfer_value -- WETH 18dp
            FROM {{ source('erc20_arbitrum', 'evt_transfer') }}
            WHERE `to` = '0x4e971a87900b931ff39d1aad67697f49835400b6' -- Fee GLP contract
                AND `contract_address` = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' -- WETH Arbitrum Smart Contract
                AND evt_block_time >= '{{project_start_date}}'
            ) a
        GROUP BY a.minute
        ) b
    )

SELECT
    x.minute,
    TRY_CAST(date_trunc('DAY', x.minute) AS date) AS block_date,
    COALESCE(x.fees_weth_generated,0) AS fees_weth_generated, -- Removes null values
    COALESCE(x.fees_weth_cumulative,0) AS fees_weth_cumulative, -- Removes null values
    x.weth_current_price
FROM
    (
    SELECT
        a.minute AS minute,
        b.weth_transfer_value AS fees_weth_generated,
        last(b.weth_cum_balance, true) OVER (ORDER BY a.minute ASC) AS fees_weth_cumulative, -- extrapolation
        last(c.weth_current_price, true) OVER (ORDER BY a.minute ASC) AS weth_current_price -- extrapolation necessary for missing values arising from in data table syncing speed
    FROM minute a
    -- since minute a is incremental filtered, the following time filters are excess
    LEFT JOIN
        (
        SELECT
            minute,
            weth_transfer_value,
            weth_cum_balance
        FROM fglp_balances
        -- excess time filter
        WHERE minute >= '{{project_start_date}}'
        ) b
        ON a.minute = b.minute
    LEFT JOIN
        (
        SELECT
            minute,
            weth_current_price
        FROM {{ref('gmx_arbitrum_glp_components_legacy')}}
        -- excess time filter
        WHERE minute >= '{{project_start_date}}'
        ) c 
        ON a.minute = c.minute
    ) x