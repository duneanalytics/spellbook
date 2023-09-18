{{ config(
        alias = alias('steth_referral_payment'),
        tags = ['dunesql'], 
        partition_by = ['period'],
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'[""ppclunghe""]\') }}'
        )
}}

--https://dune.com/queries/2348586
--ref{{'lido_accounting_dai_referral_payment'}}

with tokens AS (
select * from (values 
    (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84)  --stETH
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


steth_referral_payments_addr AS (
    SELECT _recipient AS address FROM {{source('lido_ethereum','AllowedRecipientsRegistry_RevShare_evt_RecipientAdded')}}
    WHERE
    (
        NOT EXISTS (SELECT _recipient FROM {{source('lido_ethereum','AllowedRecipientsRegistry_RevShare_evt_RecipientRemoved')}})
        OR (
            EXISTS (SELECT _recipient FROM {{source('lido_ethereum','AllowedRecipientsRegistry_RevShare_evt_RecipientRemoved')}})
            AND 
            _recipient NOT IN (SELECT _recipient FROM {{source('lido_ethereum','AllowedRecipientsRegistry_RevShare_evt_RecipientRemoved')}})
        )
    ) 
    
),



referral_payment_txns AS (
    SELECT  evt_block_time,
            evt_tx_hash,  
            contract_address,
            cast(value as double) as value
    FROM  {{source('erc20_ethereum','evt_transfer')}}
    WHERE "from" in (select address from multisigs_list where chain = 'Ethereum' and name = 'Aragon')
    AND to IN (
        SELECT address FROM steth_referral_payments_addr
    )
    AND evt_block_time >= CAST('2023-08-01 00:00' AS TIMESTAMP) 
    AND contract_address in (select address from tokens)

    UNION ALL 

    SELECT  evt_block_time,
            evt_tx_hash,  
            contract_address,
            -cast(value as double)
    FROM  {{source('erc20_ethereum','evt_transfer')}}
    WHERE to in (select address from multisigs_list where chain = 'Ethereum' and name = 'Aragon')
    AND "from" IN (
        SELECT address FROM steth_referral_payments_addr
    )
    AND evt_block_time >= CAST('2023-08-01 00:00' AS TIMESTAMP) 
    AND contract_address in (select address from tokens)
    ORDER BY evt_block_time
) 


    SELECT  evt_block_time as period,
            evt_tx_hash,  
            contract_address AS token,
            value AS amount_token
    FROM referral_payment_txns