{{ config(
        alias = 'fundraising',
         
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["pipistrella", "adcv", "zergil1397", "lido"]\') }}'
        )
}}

with tokens AS (
select * from (values 
    (0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32), --LDO
    (0x6B175474E89094C44Da98b954EedeAC495271d0F),   --DAI
    (0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48),   --USDC
    (0xdAC17F958D2ee523a2206206994597C13D831ec7), -- USDT
    (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2),   --WETH
    (0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0),   --MATIC
    (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84),  --stETH
    (0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0) --wstETH
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

fundraising_txs AS (
    select
        evt_block_time, 
        value, 
        evt_tx_hash, 
        contract_address
    FROM {{source('erc20_ethereum','evt_transfer')}}
    WHERE contract_address IN (SELECT address FROM tokens)
    AND to IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('Aragon','FinanceOpsMsig') AND chain = 'Ethereum'
    )
    AND "from" IN (SELECT address FROM diversifications_addresses)    
    AND  contract_address != 0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32
)


    SELECT
        evt_block_time AS period, 
        contract_address AS token,
        value AS amount_token,
        evt_tx_hash
    FROM fundraising_txs
    
    
    UNION ALL
    --ETH inflow
    SELECT  
        block_time AS period,
        0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token,
        tr.value,
        tx_hash
    FROM {{source('ethereum','traces')}} tr
    WHERE tr.success = True
    AND tr.to IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('Aragon','FinanceOpsMsig') AND chain = 'Ethereum'
    )
    AND tr."from" IN ( SELECT address FROM diversifications_addresses    )
    AND tr.type='call'
    AND (tr.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR tr.call_type IS NULL)

