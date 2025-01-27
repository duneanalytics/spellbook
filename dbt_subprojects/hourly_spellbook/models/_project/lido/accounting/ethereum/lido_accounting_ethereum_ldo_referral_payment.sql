{{ config(
        schema='lido_accounting_ethereum',
        alias = 'ldo_referral_payment',

        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["pipistrella", "adcv", "zergil1397"]\') }}'
        )
}}

with multisigs_list AS (
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
    (0x834560f580764bc2e0b16925f8bf229bb00cb759,  'Ethereum',  'TRPMsig'),
    (0x606f77BF3dd6Ed9790D9771C7003f269a385D942,  'Ethereum',  'AllianceMsig')
    ) as list(address, chain, name)

),

ldo_referral_payments_addr AS (
    select * from  (values
    (0x558247e365be655f9144e1a0140d793984372ef3),
    (0x6DC9657C2D90D57cADfFB64239242d06e6103E43),
    (0xDB2364dD1b1A733A690Bf6fA44d7Dd48ad6707Cd),
    (0x586b9b2F8010b284A0197f392156f1A7Eb5e86e9),
    (0xC976903918A0AF01366B31d97234C524130fc8B1),
    (0x53773e034d9784153471813dacaff53dbbb78e8c),
    (0x883f91D6F3090EA26E96211423905F160A9CA01d),
    (0xf6502Ea7E9B341702609730583F2BcAB3c1dC041),
    (0x82AF9d2Ea81810582657f6DC04B1d7d0D573F616),
    (0x351806B55e93A8Bcb47Be3ACAF71584deDEaB324),
    (0x9e2b6378ee8ad2A4A95Fe481d63CAba8FB0EBBF9),
    (0xaf8aE6955d07776aB690e565Ba6Fbc79B8dE3a5d) --rhino
    ) as list(address)
),



ldo_referral_payment_txns AS ( --only LDO referral program, need to add DAI referrals
   SELECT evt_block_time, CAST(_amount AS DOUBLE) AS amnt, evt_tx_hash, _to, _from, contract_address
    FROM {{source('lido_ethereum','LDO_evt_Transfer')}}
    WHERE _from IN (
        SELECT address FROM multisigs_list WHERE name IN ('Aragon', 'FinanceOpsMsig') AND chain = 'Ethereum'
    )
    AND _to IN (
        SELECT address FROM ldo_referral_payments_addr
    )
    ORDER BY evt_block_time

)

    SELECT  evt_block_time AS period,
            evt_tx_hash,
            contract_address AS token,
            amnt AS amount_token
    FROM ldo_referral_payment_txns
    WHERE amnt != 0

