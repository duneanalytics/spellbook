{{ config(
        schema='lido_accounting_ethereum',
        alias = 'fundraising',

        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["pipistrella", "adcv", "zergil1397", "hosuke"]\') }}'
        )
}}

WITH RECURSIVE tokens AS (
    -- Define token addresses used in the query
    SELECT address FROM (VALUES
        (0x6B175474E89094C44Da98b954EedeAC495271d0F),   -- DAI
        (0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48),   -- USDC
        (0xdAC17F958D2ee523a2206206994597C13D831ec7),    -- USDT
        (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2),    -- WETH
        (0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0),    -- MATIC
        (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84),    -- stETH
        (0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0)     -- wstETH
    ) AS t(address)
),

-- Target multisig addresses for fundraising
target_multisigs AS (
    SELECT DISTINCT address
    FROM (VALUES
        (0x3e40d73eb977dc6a537af587d48316fee66e9c8c, 'Aragon'),
        (0x48F300bD3C52c7dA6aAbDE4B683dEB27d38B9ABb, 'FinanceOpsMsig')
    ) AS m(address, name)
),

-- Source addresses for diversification
diversification_sources AS (
    SELECT address FROM (VALUES
        (0x489f04eeff0ba8441d42736549a1f1d6cca74775),  -- 1st round part 1
        (0x689e03565e36b034eccf12d182c3dc38b2bb7d33),  -- 1st round part 2
        (0xA9b2F5ce3aAE7374a62313473a74C98baa7fa70E)   -- 2nd round
    ) AS d(address)
),

-- Token transfer events
token_transfers AS (
    SELECT
        evt_block_time AS period,
        contract_address AS token,
        value AS amount_token,
        evt_tx_hash
    FROM {{source('erc20_ethereum','evt_Transfer')}} evt
    WHERE evt.contract_address IN (SELECT address FROM tokens)
        AND evt.to IN (SELECT address FROM target_multisigs)
        AND evt."from" IN (SELECT address FROM diversification_sources)
),

-- ETH transfer traces
eth_transfers AS (
    SELECT
        block_time AS period,
        0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token,  -- Using WETH address for ETH
        value AS amount_token,
        tx_hash AS evt_tx_hash
    FROM {{source('ethereum','traces')}} tr
    WHERE tr.to IN (SELECT address FROM target_multisigs)
        AND tr."from" IN (SELECT address FROM diversification_sources)
        AND tr.success = True
        AND tr.type = 'call'
        AND (tr.call_type NOT IN ('delegatecall', 'callcode', 'staticcall')
             OR tr.call_type IS NULL)
)

-- Combine token and ETH transfers
SELECT * FROM token_transfers
UNION ALL
SELECT * FROM eth_transfers