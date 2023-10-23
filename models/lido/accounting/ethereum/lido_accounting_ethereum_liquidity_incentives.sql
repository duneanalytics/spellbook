{{ config(
        alias = 'liquidity_incentives',
         
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["pipistrella", "adcv", "zergil1397", "lido"]\') }}'
        )
}}
--https://dune.com/queries/2011977
--ref{{'lido_accounting_ethereum_liquidity_incentives'}}

with tokens AS (
select * from (values 
    (0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32), --LDO
    (0xfdb794692724153d1488ccdbe0c56c252596735f), --Opti LDO 
    (0x13ad51ed4f1b7e9dc168d8a00cb3f4ddd85efa60), --Arbi LDO
    (0x6B175474E89094C44Da98b954EedeAC495271d0F),   --DAI
    (0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48),   --USDC
    (0xdAC17F958D2ee523a2206206994597C13D831ec7),   --USDT
    (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2),   --WETH
    (0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0),   --MATIC
    (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84), --stETH
    (0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0),   --wstETH
    (0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb),-- Opti wstETH 
    (0x5979D7b546E38E414F7E9822514be443A4800529) --Arbi wstETH
) as tokens(address)),



multisigs_list AS (
select * from (values
(0x3e40d73eb977dc6a537af587d48316fee66e9c8c, 'Ethereum', 'Aragon'),
(0x48F300bD3C52c7dA6aAbDE4B683dEB27d38B9ABb, 'Ethereum', 'FinanceOpsMsig'),
(0x87D93d9B2C672bf9c9642d853a8682546a5012B5, 'Ethereum', 'LiquidityRewardsMsig'),
(0x753D5167C31fBEB5b49624314d74A957Eb271709, 'Ethereum', 'LiquidityRewardMngr'),--Curve Rewards Manager 
(0x1dD909cDdF3dbe61aC08112dC0Fdf2Ab949f79D8, 'Ethereum', 'LiquidityRewardMngr'), --Balancer Rewards Manager V1 
(0x55c8De1Ac17C1A937293416C9BCe5789CbBf61d1, 'Ethereum', 'LiquidityRewardMngr'), --Balancer Rewards Manager V2 
(0x86F6c353A0965eB069cD7f4f91C1aFEf8C725551, 'Ethereum', 'LiquidityRewardMngr'), --Balancer Rewards Manager V3 
(0xf5436129Cf9d8fa2a1cb6e591347155276550635,  'Ethereum', 'LiquidityRewardMngr'),--1inch Reward Manager 
(0xE5576eB1dD4aA524D67Cf9a32C8742540252b6F4,  'Ethereum', 'LiquidityRewardMngr'), --Sushi Reward Manager
(0x87D93d9B2C672bf9c9642d853a8682546a5012B5,  'Polygon',  'LiquidityRewardsMsig'),
(0x9cd7477521B7d7E7F9e2F091D2eA0084e8AaA290,  'Ethereum', 'PolygonTeamRewardsMsig'),
(0x5033823f27c5f977707b58f0351adcd732c955dd,  'Optimism', 'LiquidityRewardsMsig'),
(0x8c2b8595ea1b627427efe4f29a64b145df439d16,  'Arbitrum', 'LiquidityRewardsMsig'),
(0x13c6ef8d45afbccf15ec0701567cc9fad2b63ce8,  'Ethereum',  'ReferralRewardsMsig'),--Solana Ref Prog Msig
(0x12a43b049A7D330cB8aEAB5113032D18AE9a9030,  'Ethereum',  'LegoMsig'),
(0x9B1cebF7616f2BC73b47D226f90b01a7c9F86956,  'Ethereum',  'ATCMsig'),
(0x17F6b2C738a63a8D3A113a228cfd0b373244633D,  'Ethereum',  'PMLMsig'),
(0xde06d17db9295fa8c4082d4f73ff81592a3ac437,  'Ethereum',  'RCCMsig'),
(0x834560f580764bc2e0b16925f8bf229bb00cb759,  'Ethereum',  'TRPMsig')
) as list(address, chain, name)
        
),

diversifications_addresses AS (
select * from  (values
(0x489f04eeff0ba8441d42736549a1f1d6cca74775, '1round_1'),
(0x689e03565e36b034eccf12d182c3dc38b2bb7d33, '1round_2'),
(0xA9b2F5ce3aAE7374a62313473a74C98baa7fa70E, '2round')
) as list(address, name)
),

intermediate_addresses AS (
select * from  (values
(0xe3224542066d3bbc02bc3d70b641be4bc6f40e36, 'Jumpgate(Solana)'),
(0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf, 'Polygon bridge'),
(0xa3a7b6f88361f48403514059f1f16c8e78d60eec, 'Arbitrum bridge'),
(0x99c9fc46f92e8a1c0dec1b1747d010903e884be1, 'Optimism bridge'),
(0x0914d4ccc4154ca864637b0b653bc5fd5e1d3ecf, 'AnySwap bridge (Polkadot, Kusama)'),
(0x3ee18b2214aff97000d974cf647e7c347e8fa585, 'Wormhole bridge'), --Solana, Terra
(0x9ee91F9f426fA633d227f7a9b000E28b9dfd8599, 'stMatic Contract')
) as list(address, name)
),

ethereum_liquidity_incentives_txns AS (
-- Ethereum Liq Incentives
    SELECT 
        evt_block_time, 
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        to, 
        "from", 
        contract_address
    FROM  {{source('erc20_ethereum','evt_transfer')}}
    WHERE "from" IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name in ('LiquidityRewardsMsig', 'LiquidityRewardMngr') AND chain = 'Ethereum'
    )
    AND to NOT IN (
        SELECT address FROM multisigs_list
        UNION ALL
        SELECT address FROM intermediate_addresses
        UNION ALL
        SELECT address FROM diversifications_addresses    
    )
    
    UNION ALL
    
    SELECT
        evt_block_time, 
        -CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        to, 
        "from", 
        contract_address 
    FROM  {{source('erc20_ethereum','evt_transfer')}}
    WHERE to IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('LiquidityRewardsMsig', 'LiquidityRewardMngr') AND chain = 'Ethereum'
    )
    AND "from" NOT IN (
        SELECT address FROM multisigs_list
        UNION ALL
        SELECT address FROM intermediate_addresses
        UNION ALL
        SELECT address FROM diversifications_addresses    
    )
    
    UNION ALL                 
    
    -- Optimism Incentives
    SELECT 
        evt_block_time, 
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        to, 
        "from", 
        contract_address
    FROM {{source('erc20_optimism','evt_transfer')}}
    WHERE  "from" IN (
        SELECT 
            address 
        FROM multisigs_list
        WHERE name IN  ('LiquidityRewardsMsig') AND chain = 'Optimism'
    )
    AND to != 0x0000000000000000000000000000000000000000
    UNION ALL
    SELECT 
        evt_block_time,
        -CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        to, 
        "from", 
        contract_address 
    FROM {{source('erc20_optimism','evt_transfer')}}
    WHERE to IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('LiquidityRewardsMsig') AND chain = 'Optimism'
    )
    AND "from" != 0x0000000000000000000000000000000000000000
    
    UNION ALL
    
    -- Arbitrum Incentives
    SELECT 
        evt_block_time, 
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        to, 
        "from", 
        contract_address
    FROM {{source('erc20_arbitrum','evt_transfer')}}
    WHERE "from" IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('LiquidityRewardsMsig') AND chain = 'Arbitrum'
    )
    AND to != 0x0000000000000000000000000000000000000000
    UNION ALL
    SELECT 
        evt_block_time, 
        -CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        to, 
        "from", 
        contract_address 
    FROM {{source('erc20_arbitrum','evt_transfer')}}
    WHERE to IN (
        SELECT 
            address
        FROM multisigs_list 
        WHERE name IN ('LiquidityRewardsMsig') AND chain = 'Arbitrum'
    )
    and "from" != 0x0000000000000000000000000000000000000000

)


    SELECT 
        evt_block_time AS period, 
        evt_tx_hash,
        value AS amount_token,
        CASE 
            WHEN contract_address = 0x0914d4ccc4154ca864637b0b653bc5fd5e1d3ecf THEN 0x5a98fcbea516cf06857215779fd812ca3bef1b32 --anyLDO
            WHEN contract_address = 0xfdb794692724153d1488ccdbe0c56c252596735f THEN 0x5a98fcbea516cf06857215779fd812ca3bef1b32 --Opti LDO 
            WHEN contract_address = 0x13ad51ed4f1b7e9dc168d8a00cb3f4ddd85efa60 THEN 0x5a98fcbea516cf06857215779fd812ca3bef1b32 --Arbi LDO 
            WHEN contract_address = 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb THEN 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0-- Opti wstETH 
            WHEN contract_address = 0x5979D7b546E38E414F7E9822514be443A4800529 THEN 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0-- Arbi wstETH 
            ELSE contract_address
        END AS token 
    FROM ethereum_liquidity_incentives_txns
    WHERE contract_address IN (SELECT address FROM tokens)
      and value != 0
    

