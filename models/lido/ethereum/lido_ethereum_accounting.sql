{{ config(
        alias ='accounting',
        partition_by = ['period'],
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido",
                                \'["pipistrella", "adcv", "zergil1397", "lido"]\') }}'
        )
}}



with tokens AS (
    SELECT LOWER('0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32') as address --LDO 
    UNION ALL
    SELECT LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F')   --DAI
    UNION ALL
    SELECT LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')   --USDC
    UNION ALL
    SELECT LOWER('0xdAC17F958D2ee523a2206206994597C13D831ec7') -- USDT
    UNION ALL
    SELECT LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')   --WETH
    UNION ALL
    SELECT LOWER('0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0')   --MATIC
    UNION ALL
    SELECT LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')   --stETH
),

multisigs_list AS (
SELECT  LOWER('0x3e40d73eb977dc6a537af587d48316fee66e9c8c') AS address,
        'Ethereum' AS chain,
        'Aragon' AS name
UNION ALL
SELECT LOWER('0x48F300bD3C52c7dA6aAbDE4B683dEB27d38B9ABb') AS address,
        'Ethereum' AS chain,
        'FinanceOpsMsig' AS name
UNION ALL
SELECT  LOWER('0x87D93d9B2C672bf9c9642d853a8682546a5012B5') AS address, 
        'Ethereum' AS chain,
        'LiquidityRewardsMsig' AS name
UNION ALL
SELECT  LOWER('0x753D5167C31fBEB5b49624314d74A957Eb271709') AS address, --Curve Rewards Manager 
        'Ethereum' AS chain,
        'LiquidityRewardMngr' AS name
UNION ALL
SELECT  LOWER('0x1dD909cDdF3dbe61aC08112dC0Fdf2Ab949f79D8') AS address, --Balancer Rewards Manager V1 
        'Ethereum' AS chain,
        'LiquidityRewardMngr' AS name
UNION ALL
SELECT  LOWER('0x55c8De1Ac17C1A937293416C9BCe5789CbBf61d1') AS address, --Balancer Rewards Manager V2 
        'Ethereum' AS chain,
        'LiquidityRewardMngr' AS name
UNION ALL
SELECT LOWER('0x86F6c353A0965eB069cD7f4f91C1aFEf8C725551') AS address, --Balancer Rewards Manager V3 
        'Ethereum' AS chain,
        'LiquidityRewardMngr' AS name
UNION ALL
SELECT LOWER('0xf5436129Cf9d8fa2a1cb6e591347155276550635') AS address, --1inch Reward Manager 
        'Ethereum' AS chain,
        'LiquidityRewardMngr' AS name
UNION ALL
SELECT LOWER('0xE5576eB1dD4aA524D67Cf9a32C8742540252b6F4') AS address, --Sushi Reward Manager
        'Ethereum' AS chain,
        'LiquidityRewardMngr' AS name
UNION ALL
SELECT  LOWER('0x87D93d9B2C672bf9c9642d853a8682546a5012B5') AS address, 
        'Polygon' AS chain,
        'LiquidityRewardsMsig' AS name
UNION ALL
SELECT  LOWER('0x9cd7477521B7d7E7F9e2F091D2eA0084e8AaA290') AS address, 
        'Ethereum' AS chain,
        'PolygonTeamRewardsMsig' AS name
    
UNION ALL
SELECT  LOWER('0x5033823f27c5f977707b58f0351adcd732c955dd') AS address, 
        'Optimism' AS chain,
        'LiquidityRewardsMsig' AS name
UNION ALL
SELECT  LOWER('0x8c2b8595ea1b627427efe4f29a64b145df439d16') AS address, 
        'Arbitrum' AS chain,
        'LiquidityRewardsMsig' AS name
UNION ALL
SELECT  LOWER('0x13c6ef8d45afbccf15ec0701567cc9fad2b63ce8') AS address, --Solana Ref Prog Msig
        'Ethereum' AS chain,
        'ReferralRewardsMsig' AS name
UNION ALL
SELECT  LOWER('0x12a43b049A7D330cB8aEAB5113032D18AE9a9030') AS address, 
        'Ethereum' AS chain,
        'LegoMsig' AS name

UNION ALL
SELECT  LOWER('0x9B1cebF7616f2BC73b47D226f90b01a7c9F86956') AS address, 
        'Ethereum' AS chain,
        'ATCMsig' AS name

UNION ALL
SELECT  LOWER('0x17F6b2C738a63a8D3A113a228cfd0b373244633D') AS address, 
        'Ethereum' AS chain,
        'PMLMsig' AS name

UNION ALL
SELECT  LOWER('0xde06d17db9295fa8c4082d4f73ff81592a3ac437') AS address, 
        'Ethereum' AS chain,
        'RCCMsig' AS name
        
UNION ALL
SELECT  LOWER('0x834560f580764bc2e0b16925f8bf229bb00cb759') AS address, 
        'Ethereum' AS chain,
        'TRPMsig' AS name
        
),

diversifications_addresses AS (
    SELECT LOWER('0x489f04eeff0ba8441d42736549a1f1d6cca74775') AS address , '1round_1' AS name
    UNION ALL
    SELECT LOWER('0x689e03565e36b034eccf12d182c3dc38b2bb7d33') AS address , '1round_2' AS name
    UNION ALL
    SELECT LOWER('0xA9b2F5ce3aAE7374a62313473a74C98baa7fa70E') AS address , '2round' AS name
),

intermediate_addresses AS (
    SELECT LOWER('0xe3224542066d3bbc02bc3d70b641be4bc6f40e36') AS address , 'Jumpgate(Solana)' as name
    UNION ALL
    SELECT LOWER('0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf'), 'Polygon bridge'
    UNION ALL
    SELECT LOWER('0xa3a7b6f88361f48403514059f1f16c8e78d60eec'), 'Arbitrum bridge'
    UNION ALL
    SELECT LOWER('0x99c9fc46f92e8a1c0dec1b1747d010903e884be1'), 'Optimism bridge'
    UNION ALL
    SELECT LOWER('0x0914d4ccc4154ca864637b0b653bc5fd5e1d3ecf'), 'AnySwap bridge (Polkadot, Kusama)'
    UNION ALL
    SELECT LOWER('0x3ee18b2214aff97000d974cf647e7c347e8fa585'), 'Wormhole bridge' --Solana, Terra
    UNION ALL
    SELECT LOWER('0x9ee91F9f426fA633d227f7a9b000E28b9dfd8599'), 'stMatic Contract'
),

ldo_referral_payments_addr AS (
    SELECT LOWER('0x558247e365be655f9144e1a0140d793984372ef3') AS address
    UNION ALL
    SELECT LOWER('0x6DC9657C2D90D57cADfFB64239242d06e6103E43')
    UNION ALL
    SELECT LOWER('0xDB2364dD1b1A733A690Bf6fA44d7Dd48ad6707Cd')
    UNION ALL
    SELECT LOWER('0x586b9b2F8010b284A0197f392156f1A7Eb5e86e9')
    UNION ALL
    SELECT LOWER('0xC976903918A0AF01366B31d97234C524130fc8B1')
    UNION ALL
    SELECT LOWER('0x53773e034d9784153471813dacaff53dbbb78e8c')
    UNION ALL
    SELECT LOWER('0x883f91D6F3090EA26E96211423905F160A9CA01d')
    UNION ALL
    SELECT LOWER('0xf6502Ea7E9B341702609730583F2BcAB3c1dC041')
    UNION ALL
    SELECT LOWER('0x82AF9d2Ea81810582657f6DC04B1d7d0D573F616')
    UNION ALL
    SELECT LOWER('0x351806B55e93A8Bcb47Be3ACAF71584deDEaB324')
    UNION ALL
    SELECT LOWER('0x9e2b6378ee8ad2A4A95Fe481d63CAba8FB0EBBF9')
    UNION ALL
    SELECT LOWER('0xaf8aE6955d07776aB690e565Ba6Fbc79B8dE3a5d') --rhino
),
dai_referral_payments_addr AS (
    SELECT _recipient AS address FROM {{ source('lido_ethereum', 'AllowedRecipientsRegistry_evt_RecipientAdded') }}
    WHERE
    (
        NOT EXISTS (SELECT _recipient FROM {{ source('lido_ethereum', 'AllowedRecipientsRegistry_evt_RecipientRemoved') }})
        OR (
            EXISTS (SELECT _recipient FROM {{ source('lido_ethereum', 'AllowedRecipientsRegistry_evt_RecipientRemoved') }})
            AND 
            _recipient NOT IN (SELECT _recipient FROM {{ source('lido_ethereum', 'AllowedRecipientsRegistry_evt_RecipientRemoved') }})
        )
    ) 
    UNION ALL
    SELECT LOWER('0xaf8aE6955d07776aB690e565Ba6Fbc79B8dE3a5d') --rhino
),

 eth_prices as (
 SELECT  DATE_TRUNC('day', minute) AS period, 
        contract_address AS token,
        symbol,
        decimals,
        price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'ethereum'
    AND contract_address = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') 
    AND EXTRACT(hour FROM minute) = 23
    AND EXTRACT(minute FROM minute) = 59
    AND minute >= '2020-10-01'
),

tokens_prices AS (

SELECT  DATE_TRUNC('day', prices.usd.minute) AS period, 
        prices.usd.contract_address AS token,
        prices.usd.symbol,
        prices.usd.decimals,
        prices.usd.price,
        eth_prices.price as eth_usd_price,
        prices.usd.price/eth_prices.price as token_eth_price
    FROM {{ source('prices', 'usd') }}
    left join eth_prices on DATE_TRUNC('day', prices.usd.minute) =  eth_prices.period
    WHERE prices.usd.blockchain = 'ethereum'
    AND prices.usd.contract_address IN (SELECT address FROM tokens)
    AND EXTRACT(hour FROM prices.usd.minute) = 23
    AND EXTRACT(minute FROM prices.usd.minute) = 59
    AND minute >= '2020-10-01'
union all

SELECT  DATE_TRUNC('day', prices.usd.minute) AS period, 
        prices.usd.contract_address, --stSOL
        'stSOL',
        0,
        prices.usd.price,
        prices.usd.price as eth_usd_price,
        prices.usd.price/eth_prices.price as token_eth_price
    FROM {{ source('prices', 'usd') }}
    left join eth_prices on DATE_TRUNC('day', prices.usd.minute) =  eth_prices.period
    WHERE prices.usd.symbol = 'stSOL' 
    AND EXTRACT(hour FROM prices.usd.minute) = 23
    AND EXTRACT(minute FROM prices.usd.minute) = 59

),

oracle_txns AS ( 
    SELECT
        evt_block_time AS period,
        (CAST(postTotalPooledEther AS DOUBLE)-CAST(preTotalPooledEther AS DOUBLE)) lido_rewards,
        evt_tx_hash
    FROM  {{ source('lido_ethereum', 'LidoOracle_evt_PostTotalShares') }}
    ORDER BY 1 DESC
),

protocol_fee AS (
    SELECT 
        DATE_TRUNC('day', evt_block_time) AS period, 
        LEAD(DATE_TRUNC('day', evt_block_time), 1, NOW()) OVER (ORDER BY DATE_TRUNC('day', evt_block_time)) AS next_period,
        CAST(feeBasisPoints AS DOUBLE)/10000 AS points
    FROM {{ source('lido_ethereum', 'steth_evt_FeeSet') }}
),

protocol_fee_distribution AS (
    SELECT 
        DATE_TRUNC('day', evt_block_time) AS period, 
        LEAD(DATE_TRUNC('day', evt_block_time), 1, NOW()) OVER (ORDER BY DATE_TRUNC('day', evt_block_time)) AS next_period,
        CAST(insuranceFeeBasisPoints AS DOUBLE)/10000 AS insurance_points,
        CAST(operatorsFeeBasisPoints AS DOUBLE)/10000 AS operators_points,
        CAST(treasuryFeeBasisPoints AS DOUBLE)/10000 AS treasury_points
    FROM {{ source('lido_ethereum', 'steth_evt_FeeDistributionSet') }}
),

revenue AS ( 
    SELECT  
        oracle_txns.period AS period, 
        oracle_txns.evt_tx_hash,
        LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') AS token,
        lido_rewards AS total,
        protocol_fee.points AS protocol_fee,
        protocol_fee_distribution.insurance_points AS insurance_fee,
        protocol_fee_distribution.operators_points AS operators_fee,
        protocol_fee_distribution.treasury_points AS treasury_fee,
        (1 - protocol_fee.points)*lido_rewards AS depositors_revenue,
        protocol_fee.points*protocol_fee_distribution.treasury_points*lido_rewards AS treasury_revenue,
        protocol_fee.points*protocol_fee_distribution.insurance_points*lido_rewards AS insurance_revenue,
        protocol_fee.points*protocol_fee_distribution.operators_points*lido_rewards AS operators_revenue
    FROM oracle_txns
    LEFT JOIN protocol_fee ON DATE_TRUNC('day', oracle_txns.period) >= protocol_fee.period AND DATE_TRUNC('day', oracle_txns.period) < protocol_fee.next_period
    LEFT JOIN protocol_fee_distribution ON DATE_TRUNC('day', oracle_txns.period) >= protocol_fee_distribution.period AND DATE_TRUNC('day', oracle_txns.period) < protocol_fee_distribution.next_period
    ORDER BY 1,2 
),

ldo_referral_payment_txns AS ( --only LDO referral program, need to add DAI referrals
    SELECT evt_block_time, CAST(_amount AS DOUBLE) AS amnt, evt_tx_hash, _to, _from, contract_address
    FROM {{ source('lido_ethereum', 'LDO_evt_Transfer') }}
    WHERE _from IN (
        SELECT address FROM multisigs_list WHERE name IN ('Aragon', 'FinanceOpsMsig') AND chain = 'Ethereum'
    )
    AND _to IN (
        SELECT address FROM ldo_referral_payments_addr
    )
    ORDER BY evt_block_time  
), 

ldo_referral_payment AS (
    SELECT  evt_block_time AS period,
            evt_tx_hash,
            contract_address AS token,
            amnt AS amount_token
    FROM ldo_referral_payment_txns 
    --GROUP BY 1, 2
),

dai_referral_payment_txns AS (
    SELECT  evt_block_time,
            evt_tx_hash,  
            contract_address,
            value
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE `from` = LOWER('0x3e40D73EB977Dc6a537aF587D48316feE66E9C8c')
    AND `to` IN (
        SELECT address FROM dai_referral_payments_addr
    )
    AND evt_block_time >= CAST('2023-01-01 00:00' AS TIMESTAMP) 
    AND contract_address = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F')
    ORDER BY evt_block_time  
), 

dai_referral_payment AS (
    SELECT  evt_block_time AS PERIOD,
            evt_tx_hash,  
            contract_address AS token,
            value AS amount_token
    FROM dai_referral_payment_txns 
    
),

ethereum_liquidity_incentives_txns AS (
-- Ethereum Liq Incentives
    SELECT 
        evt_block_time, 
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        `to`, 
        `from`, 
        contract_address
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE `from` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name in ('LiquidityRewardsMsig', 'LiquidityRewardMngr') AND chain = 'Ethereum'
    )
    AND `to` NOT IN (
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
        `to`, 
        `from`, 
        contract_address 
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE `to` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('LiquidityRewardsMsig', 'LiquidityRewardMngr') AND chain = 'Ethereum'
    )
    AND `from` NOT IN (
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
        `to`, 
        `from`, 
        contract_address
    FROM  {{ source('erc20_optimism', 'evt_Transfer') }}
    WHERE `from` IN (
        SELECT 
            address 
        FROM multisigs_list
        WHERE name IN  ('LiquidityRewardsMsig') AND chain = 'Optimism'
    )
    AND `to` != LOWER('0x0000000000000000000000000000000000000000')
    UNION ALL
    SELECT 
        evt_block_time,
        -CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        `to`, 
        `from`, 
        contract_address 
    FROM {{ source('erc20_optimism', 'evt_Transfer') }}
    WHERE `to` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('LiquidityRewardsMsig') AND chain = 'Optimism'
    )
    AND `from` != LOWER('0x0000000000000000000000000000000000000000')
    
    UNION ALL
    
    -- Arbitrum Incentives
    SELECT 
        evt_block_time, 
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        `to`, 
        `from`, 
        contract_address
    FROM {{ source('erc20_arbitrum', 'evt_Transfer') }}
    WHERE `from` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('LiquidityRewardsMsig') AND chain = 'Arbitrum'
    )
    AND `to` != LOWER('0x0000000000000000000000000000000000000000')
    UNION ALL
    SELECT 
        evt_block_time, 
        -CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        `to`, 
        `from`, 
        contract_address 
    FROM {{ source('erc20_arbitrum', 'evt_Transfer') }}
    WHERE `to` IN (
        SELECT 
            address
        FROM multisigs_list 
        WHERE name IN ('LiquidityRewardsMsig') AND chain = 'Arbitrum'
    )
    and `from` != LOWER('0x0000000000000000000000000000000000000000')

), 

ethereum_liquidity_incentives AS (
    SELECT 
        evt_block_time AS period, 
        evt_tx_hash,
        value AS amount_token,
        CASE 
            WHEN contract_address = LOWER('0x0914d4ccc4154ca864637b0b653bc5fd5e1d3ecf') THEN LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32') --anyLDO
            WHEN contract_address = LOWER('0xfdb794692724153d1488ccdbe0c56c252596735f') THEN LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32') --Opti LDO 
            WHEN contract_address = LOWER('0x13ad51ed4f1b7e9dc168d8a00cb3f4ddd85efa60') THEN LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32') --Arbi LDO 
            ELSE contract_address
        END AS token 
    FROM ethereum_liquidity_incentives_txns
    WHERE contract_address IN (SELECT address FROM tokens)
    
),

lox_incentives_txns AS (
-- Polygon Incentives
    select 
        evt_block_time, 
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        `to`, 
        `from`, 
        contract_address
    FROM  {{ source('erc20_polygon', 'evt_Transfer') }}
    WHERE `from` IN (
        SELECT
            address 
        FROM multisigs_list 
        WHERE name IN  ('LiquidityRewardsMsig') AND chain = 'Polygon'
    )
    AND `to` != LOWER('0x0000000000000000000000000000000000000000')
    
    UNION ALL
    
    SELECT 
        evt_block_time, 
        -CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        `to`, 
        `from`, 
        contract_address 
    FROM {{ source('erc20_polygon', 'evt_Transfer') }}
    WHERE `to` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN  ('LiquidityRewardsMsig') AND chain = 'Polygon'
    )
    AND `from` != LOWER('0x0000000000000000000000000000000000000000')
    UNION ALL
    SELECT
        evt_block_time,
        CAST(value AS DOUBLE), 
        evt_tx_hash, 
        `to`, 
        `from`, 
        contract_address
    FROM {{ source('erc20_polygon', 'evt_Transfer') }}
    WHERE `from` IN (
        SELECT 
            address
        FROM multisigs_list 
        WHERE name IN  ('PolygonTeamRewardsMsig' ) and chain = 'Ethereum'
    )
    AND `to` NOT IN (
        SELECT address FROM multisigs_list
        UNION ALL
        SELECT address FROM intermediate_addresses
    )
    UNION ALL
    SELECT 
        evt_block_time, 
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        `to`, 
        `from`, 
        contract_address
    FROM {{ source('erc20_polygon', 'evt_Transfer') }}
    WHERE `to` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('PolygonTeamRewardsMsig' ) AND chain = 'Ethereum'
    )
    AND `from` NOT IN (
        SELECT address FROM multisigs_list
        UNION ALL
        SELECT address FROM intermediate_addresses
    )
    
    UNION ALL
    
    -- Solana, Terra Liq Incentives
    SELECT 
        evt_block_time, 
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        `to`, 
        `from`, 
        contract_address
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE `from` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('LiquidityRewardsMsig', 'Aragon', 'ReferralRewardsMsig') AND chain = 'Ethereum'
    )
    AND `to` IN (
        SELECT 
            address 
        FROM intermediate_addresses 
        WHERE name IN ('Jumpgate(Solana)','Wormhole bridge') 
    )
    UNION ALL
    SELECT
        evt_block_time, 
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        `to`, 
        `from`, 
        contract_address 
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE `to` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('LiquidityRewardsMsig', 'Aragon', 'ReferralRewardsMsig') and chain = 'Ethereum'
    )
    AND `from` IN (
        SELECT 
            address 
        FROM intermediate_addresses 
        WHERE name IN ('Jumpgate(Solana)','Wormhole bridge') 
    )
    UNION ALL
    -- Polkadot, Kusama Incentives
    SELECT 
        evt_block_time, 
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        `to`, 
        `from`, 
        contract_address
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE `from` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('LiquidityRewardsMsig') AND chain = 'Ethereum'
    )
    AND `to` IN (
        SELECT 
            address 
        FROM intermediate_addresses 
        WHERE name IN ('AnySwap bridge (Polkadot, Kusama)') 
    )
    UNION ALL
    SELECT 
        evt_block_time, 
        -CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        `to`, 
        `from`, 
        contract_address 
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE `to` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('LiquidityRewardsMsig') and chain = 'Ethereum'
    )
    AND `from` IN (
        SELECT 
            address 
        FROM intermediate_addresses 
        WHERE name IN ('AnySwap bridge (Polkadot, Kusama)') 
    )
), 

lox_incentives AS (
    SELECT 
        evt_block_time AS period, 
        evt_tx_hash,
        value AS amount_token,
        CASE
            WHEN contract_address = LOWER('0x0914d4ccc4154ca864637b0b653bc5fd5e1d3ecf') THEN LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32') --anyLDO
            WHEN contract_address = LOWER('0xc3c7d422809852031b44ab29eec9f1eff2a58756') THEN LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32') --Poly LDO 
        ELSE contract_address
        END AS token     
    
    FROM lox_incentives_txns
    WHERE contract_address IN (SELECT address FROM tokens)
    
),

lego_expenses_txns AS (
    select
        evt_block_time,
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        contract_address 
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE contract_address IN (SELECT address FROM tokens)
    AND `from` IN (
        SELECT
            address 
        FROM multisigs_list 
        WHERE name = 'LegoMsig' AND chain = 'Ethereum'
    )
    AND `to` NOT IN (SELECT address FROM multisigs_list
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
        contract_address
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE contract_address IN (SELECT address FROM tokens)
    AND `to` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name = 'LegoMsig' AND chain = 'Ethereum')
    AND `from` NOT IN (SELECT address FROM multisigs_list
    UNION ALL
    SELECT address FROM intermediate_addresses
    UNION ALL
    SELECT address FROM diversifications_addresses    
    )    
), 

lego_expenses AS (
    SELECT  
        evt_block_time AS period, 
        contract_address AS token,
        value AS amount_token,
        evt_tx_hash
    FROM lego_expenses_txns
    WHERE contract_address IN (SELECT address FROM tokens)
), 

operating_expenses_txns AS ( --all transfers out of recognized entities multisigs (3.2.4.1.)
    SELECT
        evt_block_time, 
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        contract_address, 
        `from`, 
        `to`
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE contract_address IN (SELECT address FROM tokens)
    AND `from` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('ATCMsig', 'PMLMsig', 'RCCMsig') AND chain = 'Ethereum'
    )
    AND `to` NOT IN (
        SELECT address FROM multisigs_list
        UNION ALL
        SELECT address FROM intermediate_addresses
        UNION ALL
        SELECT address FROM ldo_referral_payments_addr  
        UNION ALL
        SELECT LOWER('0x0000000000000000000000000000000000000000')
        UNION ALL
        SELECT address FROM diversifications_addresses    
    )
    UNION ALL
    --ETH outflow
    SELECT
        block_time,
        CAST(tr.value AS DOUBLE) AS value,
        tx_hash,
        LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'), 
        `from`, 
        `to`
    FROM {{ source('ethereum', 'traces') }} tr 
    LEFT JOIN tokens_prices ON DATE_TRUNC('day', tr.block_time) = tokens_prices.period 
    AND tokens_prices.token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
    
    
    AND tr.`success`= True
    AND tr.`from` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('ATCMsig', 'PMLMsig', 'RCCMsig') AND chain = 'Ethereum'
    )
    AND tr.`type`='call'
    AND (tr.`call_type` NOT IN ('delegatecall', 'callcode', 'staticcall') OR tr.`call_type` IS NULL)

), 

operating_expenses AS (
    SELECT
        evt_block_time AS period, 
        contract_address AS token,
        value AS amount_token,
        evt_tx_hash
    FROM operating_expenses_txns
    WHERE contract_address IN (SELECT address FROM tokens)
    
), 

other_expenses_txns AS (
    SELECT 
        evt_block_time,
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        contract_address
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE contract_address IN (SELECT address FROM tokens)
        AND `from` IN (
            SELECT 
                address 
            FROM multisigs_list
            WHERE name IN ('Aragon','FinanceOpsMsig') AND chain = 'Ethereum'
        )
        AND `to` NOT IN (
            SELECT address FROM multisigs_list
            UNION ALL
            SELECT address FROM intermediate_addresses
            UNION ALL
            SELECT address FROM ldo_referral_payments_addr  
            UNION ALL
            SELECT LOWER('0x0000000000000000000000000000000000000000')
            UNION ALL
            SELECT address FROM diversifications_addresses    
    )    
    UNION ALL
    --ETH outflow
    SELECT  
        block_time,
        CAST(tr.value AS DOUBLE) AS value,
        tx_hash,
        LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
    FROM {{ source('ethereum', 'traces') }} tr
    LEFT JOIN tokens_prices ON DATE_TRUNC('day', tr.block_time) = tokens_prices.period 
    AND tokens_prices.token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
   
    
    AND tr.`success` = True
    AND tr.`from` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('Aragon','FinanceOpsMsig') AND chain = 'Ethereum'
    )
    AND tr.`to` NOT IN (
        SELECT 
            address 
        FROM multisigs_list
        UNION ALL 
        SELECT address FROM diversifications_addresses)
        AND tr.`type`='call'
        AND (tr.`call_type` NOT IN ('delegatecall', 'callcode', 'staticcall') OR tr.`call_type` IS NULL
    )


), 

other_expenses AS (
    SELECT  
        evt_block_time AS period, 
        contract_address AS token,
        value AS amount_token,
        evt_tx_hash
    FROM other_expenses_txns
    WHERE contract_address IN (SELECT address FROM tokens)
    
), 

trp_expenses_txns AS (
    SELECT 
        evt_block_time,
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        contract_address
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE contract_address = LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32')
        AND `from` IN (
            SELECT 
                address 
            FROM multisigs_list
            WHERE name IN ('Aragon','FinanceOpsMsig') AND chain = 'Ethereum'
        )
        AND `to` IN (
            SELECT 
                address 
            FROM multisigs_list
            WHERE name IN ('TRPMsig') AND chain = 'Ethereum'
    )    
    
), 

trp_expenses AS (
    SELECT  
        evt_block_time AS period, 
        contract_address AS token,
        value AS amount_token,
        evt_tx_hash
    FROM trp_expenses_txns
    
), 


other_income_txns AS (
    SELECT
        evt_block_time, 
        CAST(value AS DOUBLE) AS value, 
        evt_tx_hash, 
        contract_address
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE contract_address IN (SELECT address FROM tokens)
  
    AND `to` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('Aragon','FinanceOpsMsig') AND chain = 'Ethereum'
    )
    AND `from` NOT IN (
        SELECT address FROM multisigs_list
        UNION ALL
        SELECT address FROM ldo_referral_payments_addr  
        UNION ALL
        select LOWER('0x0000000000000000000000000000000000000000')
        UNION ALL
        SELECT address FROM diversifications_addresses    
    )    

),

--Solana stSOL income--

stsol_income_txs AS (
    select 
        tx_id, 
        block_time AS period, 
        block_slot, 
        pre_token_balance, 
        post_token_balance, 
        token_balance_change AS delta
    FROM {{ source('solana', 'account_activity') }}
    WHERE  block_time >= CAST('2021-11-01' AS TIMESTAMP)
    AND pre_token_balance IS NOT NULL
    AND token_mint_address =  '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj'
    AND address =  'CYpYPtwY9QVmZsjCmguAud1ctQjXWKpWD7xeL5mnpcXk'
    AND token_balance_change > 0
    ORDER BY block_time DESC
),

stsol_income AS (
    SELECT  
            i.period AS period,
            '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj' AS token,
            COALESCE(delta,0) AS amount_token,
            tx_id as evt_tx_hash
    FROM  stsol_income_txs i 
    
),

other_income AS (
    SELECT
        evt_block_time AS period, 
        contract_address AS token,
        value AS amount_token,
        evt_tx_hash
    FROM other_income_txns
    
    UNION ALL
    --ETH inflow
    SELECT
        block_time AS time,
        LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS token,
        CAST(tr.value AS DOUBLE),
        tx_hash
    FROM  {{ source('ethereum', 'traces') }} tr
    WHERE tr.success = True
   
    AND tr.`to` in (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('Aragon','FinanceOpsMsig') AND chain = 'Ethereum'
    )
    AND tr.`from` NOT IN ( 
        SELECT address FROM multisigs_list
        UNION ALL 
        SELECT address FROM diversifications_addresses    
    )
    AND tr.`type`='call'
    AND (tr.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR tr.call_type IS NULL)
    --GROUP BY 1
    UNION --stSOL to solana treasury
    SELECT
        period, 
        '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj' AS token,
        amount_token,
        evt_tx_hash
    FROM stsol_income
),

fundraising_txs AS (
    select
        evt_block_time, 
        value, 
        evt_tx_hash, 
        contract_address
    FROM {{ source('erc20_ethereum', 'evt_Transfer') }}
    WHERE contract_address IN (SELECT address FROM tokens)
    AND `to` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('Aragon','FinanceOpsMsig') AND chain = 'Ethereum'
    )
    AND `from` IN (SELECT address FROM diversifications_addresses)    
    AND  contract_address != LOWER('0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32')
),

fundraising AS (
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
        LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS token,
        tr.value,
        tx_hash
    FROM {{ source('ethereum', 'traces') }} tr
    WHERE tr.success = True
    
    AND tr.`to` IN (
        SELECT 
            address 
        FROM multisigs_list 
        WHERE name IN ('Aragon','FinanceOpsMsig') AND chain = 'Ethereum'
    )
    AND tr.`from` IN ( SELECT address FROM diversifications_addresses    )
    AND tr.`type`='call'
    AND (tr.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR tr.call_type IS NULL)
    --GROUP BY 1
),

deposits AS (
    SELECT 
        block_time AS period,
        amount_staked*POWER(10,18) AS amount_staked,
        LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS token, --ETH
        tx_hash
    FROM {{ ref('staking_ethereum_deposits') }} 
    WHERE depositor_entity = 'Lido'
    
    
),

accounts AS (
--GROUP BY 1,2,3,4,5,6,8,9, tokens_prices.decimals, tokens_prices.price, tokens_prices.token_eth_price
    SELECT  accounts.period, 
            accounts.evt_tx_hash as hash,
            accounts.primary_label,
            accounts.secondary_label,
            accounts.account,
            accounts.category,
            
            SUM(coalesce(accounts.token_amount, 0))/coalesce(POWER(10,tokens_prices.decimals),1) AS value_base_token,
            
            CASE WHEN pt.symbol = 'WETH' THEN 'ETH' 
                 WHEN tokens_prices.token = '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj' THEN 'stSOL'
                 ELSE pt.symbol END AS base_token,
            coalesce(tokens_prices.token, accounts.token) AS base_token_address,
            
            coalesce(SUM(accounts.token_amount*tokens_prices.price)/POWER(10,tokens_prices.decimals), 0) AS value_usd,
            case when coalesce(tokens_prices.token, accounts.token) = lower('0xae7ab96520de3a18e5e111b5eaab095312d7fe84') 
                 then SUM(coalesce(accounts.token_amount, 0))/coalesce(POWER(10,tokens_prices.decimals),1) 
                 else coalesce(SUM(accounts.token_amount*tokens_prices.token_eth_price)/POWER(10,tokens_prices.decimals), 0) 
            end AS value_eth,
            coalesce(tokens_prices.price, 0) as token_price,
            coalesce(tokens_prices.token_eth_price, 0) as token_eth_price

    FROM (
    -- Staked ETH
    SELECT  period,
            tx_hash as evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.1. Staked Assets' AS secondary_label,
            '1.1.1. Staked ETH' AS account,
            '-' AS category,
            COALESCE(amount_staked,0) AS token_amount,
            coalesce(token, LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')) as token --ETH
    FROM deposits
    
    
    UNION ALL
    
    select  period,
            tx_hash,
            '2. Liabilities' AS primary_label,
            '2.1. Staked Assets' AS secondary_label,
            '2.1.1. stETH in Circulation' AS account,
            '-' AS category,
            COALESCE(amount_staked,0) AS token_amount,
            LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') AS token
    FROM deposits
    
    -- ========================================================= Gross staking rewards with the corresponding increase in asset accounts
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.1. Net Revenue' AS account,
            '3.2.1.1. Gross staking rewards (+)' AS category,
            COALESCE(revenue.total,0) AS token_amount,
            token
    FROM revenue
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            '1.3.1.1. stETH' AS category,
            COALESCE(revenue.total,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM revenue
    
    
    
    UNION ALL
    
    SELECT  period, 
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.1. Staked Assets' AS secondary_label,
            '1.1.1. Staked ETH' AS account,
            '-' AS category,
            COALESCE(revenue.total,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM revenue
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '2. Liabilities' AS primary_label,
            '2.1. Staked Assets' AS secondary_label,
            '2.1.1. stETH in Circulation' AS account,
            '-' AS category,
            COALESCE(revenue.total,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM revenue
    
    UNION ALL
    
    -- ========================================================= Gross staking rewards sent to holders with the corresponding decrease in assets
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.1. Net Revenue' AS account,
            '3.2.1.2. Staking rewards to holders (-)' AS category,
            
            -COALESCE(revenue.depositors_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM revenue
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            '1.3.1.1. stETH' AS category,
            -COALESCE(revenue.depositors_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM revenue
    
    UNION ALL
    
    -- ========================================================= Cost of revenue to node operators
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.2. Cost of Revenue' AS account,
            '3.2.2.1. Staking rewards to node operators (-)' AS category,
            
            -COALESCE(operators_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM revenue
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            '1.3.1.1. stETH' AS category,
            -COALESCE(operators_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM revenue
    
    UNION ALL
    
    -- ========================================================= Provisions for slashing insurance set aside 
    -- On the same side of the balance sheet, a decrease in equity must be associated with an increase in liabilities to balance
    -- Slashing insurance should go to an effective liability account through the following steps:
    -- 1. Recognize the expense and the contra asset account 
    -- 2. Recognize the insurance liability and the associated asset account
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.2. Cost of Revenue' AS account,
            '3.2.2.2. Provision for slashing insurance (-)' AS category,
            
            -COALESCE(insurance_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM revenue
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.2. Slashing Insurance Fund' AS secondary_label,
            '1.2.2. Slashing Insurance Contra Assets' AS account,
            '-' AS category,
            -COALESCE(insurance_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM revenue
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '2. Liabilities' AS primary_label,
            '2.2. Slashing Insurance Fund' AS secondary_label,
            '2.2.1. Slashing Insurance' AS account,
            '-' AS category,
            COALESCE(insurance_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM revenue
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.2. Slashing Insurance Fund' AS secondary_label,
            '1.2.1. Slashing Insurance Assets' AS account,
            '-' AS category,
            COALESCE(insurance_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM revenue
    
    UNION ALL
    
    -- ========================================================= Other expenses
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.2. Cost of Revenue' AS account,
            '3.2.2.3. Other costs of revenue (-)' AS category,

            -0,
            LOWER('0xae7ab96520de3a18e5e111b5eaab095312d7fe84') AS base_token
    FROM revenue
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            '1.3.1.1. stETH' AS category,
            
            -0,
            LOWER('0xae7ab96520de3a18e5e111b5eaab095312d7fe84') AS base_token
    FROM revenue
    
    -- ========================================================= LDO denominated Deposit Referrals
    -- This only works for LDO denominated expenses.
    -- Hypothetical DAI or stETH expenses in the same category would have to hit the Surplus and need a separate think
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.3. Sales & Marketing Incentives' AS account,
            '3.2.3.2. Deposit Referrals' AS category,
            -COALESCE(CAST(amount_token AS DOUBLE), 0),
            token
    FROM ldo_referral_payment
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.3. LDO token transactions' AS secondary_label,
            '3.3.1. LDO Contra Equity' AS account,
            '3.3.1.2. Deposit Referrals' AS category,
            COALESCE(CAST(amount_token AS DOUBLE), 0),
            token
    FROM ldo_referral_payment
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.3. Sales & Marketing Incentives' AS account,
            '3.2.3.2. Deposit Referrals' AS category,
            
            -COALESCE(CAST(amount_token AS DOUBLE), 0),
            token
    FROM dai_referral_payment
    
    UNION ALL 
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' as primary_label,
            '1.3. Protocol Assets' as secondary_label,
            '1.3.1. Protocol Assets' as account,
            '1.3.1.2. DAI' AS category,
            
            -COALESCE(CAST(amount_token AS DOUBLE), 0),
            token
    FROM dai_referral_payment
    
    
    UNION ALL
    
    -- ========================================================= LDO denominated Liquidity Incentives
    -- This only works for LDO denominated expenses.
    -- Hypothetical DAI or stETH expenses in the same category would have to hit the Surplus and need a separate think
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.3. Sales & Marketing Incentives' AS account,
            '3.2.3.1. Liquidity Rewards' AS category,
            
            -COALESCE(ethereum_liquidity_incentives.amount_token,0),
            token
    FROM ethereum_liquidity_incentives
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' as primary_label,
            '3.3. LDO token transactions' as secondary_label,
            '3.3.1. LDO Contra Equity' as account,
            '3.3.1.1. Liquidity Rewards' as category,
            
            COALESCE(ethereum_liquidity_incentives.amount_token,0),
            token
    FROM ethereum_liquidity_incentives
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' as primary_label,
            '1.3. Protocol Assets' as secondary_label,
            '1.3.1. Protocol Assets' as account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            
            COALESCE(ethereum_liquidity_incentives.amount_token,0),
            token
            
    FROM ethereum_liquidity_incentives
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    -- ========================================================= LDO denominated Domain Incentives
    -- This only works for LDO denominated expenses.
    -- Hypothetical DAI or stETH expenses in the same category would have to hit the Surplus and need a separate think
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.3. Sales & Marketing Incentives' AS account,
            '3.2.3.3. Domain Incentives' AS category,
            
            -lox_incentives.amount_token AS value,
            token
    FROM lox_incentives
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.3. LDO token transactions' AS secondary_label,
            '3.3.1. LDO Contra Equity' AS account,
            '3.3.1.3. Domain Incentives' AS category,
            
            lox_incentives.amount_token AS value,
            token
    FROM lox_incentives
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            
            lox_incentives.amount_token AS value,
            token
    FROM lox_incentives
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    -- ========================================================= LEGO Grants
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.4. Operating expenses' AS account,
            '3.2.4.2. LEGO grants' AS category,
            
            -lego_expenses.amount_token,
            token 
    FROM lego_expenses
    
    
    UNION ALL
    
    select  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            
            -lego_expenses.amount_token,
            token
    FROM lego_expenses
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    UNION ALL 
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.3. LDO token transactions' AS secondary_label,
            '3.3.1. LDO Contra Equity' AS account,
            '3.3.1.5. LEGO' AS category,
            
            lego_expenses.amount_token,
            token
    FROM lego_expenses
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    UNION ALL
    -- ========================================================= Operating expenses from service entities
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.4. Operating expenses' AS account,
            '3.2.4.1. Operating expenses' AS category,
            
            -operating_expenses.amount_token AS value,
            token 
    FROM operating_expenses
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            
            -operating_expenses.amount_token,
            token
    FROM operating_expenses
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.3. LDO token transactions' AS secondary_label,
            '3.3.1. LDO Contra Equity' AS account,
            '3.3.1.6. Other' AS category,
            
            operating_expenses.amount_token,
            token
    FROM operating_expenses
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    UNION ALL
    
    -- ========================================================= All other operating expenses
    --not-LDO expenses
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.4. Operating expenses' AS account,
            '3.2.4.4. Other' AS category,
            
            -other_expenses.amount_token,
            token 
    FROM other_expenses
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            
            -other_expenses.amount_token,
            token
    FROM other_expenses 
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    --LDO expenses
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.4. Operating expenses' AS account,
            '3.2.4.1. Operating expenses' AS category,
            
            -other_expenses.amount_token,
            token 
    FROM other_expenses
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    UNION ALL
    
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.3. LDO token transactions' AS secondary_label,
            '3.3.1. LDO Contra Equity' AS account,
            '3.3.1.6. Other' AS category,
            
            other_expenses.amount_token,
            token
    FROM    other_expenses
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.4. Operating expenses' AS account,
            '3.2.4.3. TRP grants' AS category,
            
            -trp_expenses.amount_token,
            token 
    FROM trp_expenses
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    
    UNION ALL
    
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.3. LDO token transactions' AS secondary_label,
            '3.3.1. LDO Contra Equity' AS account,
            '3.3.1.6. Other' AS category,
            
            trp_expenses.amount_token,
            token
    FROM trp_expenses
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    UNION ALL
    -- ========================================================= Non standard protocol surplus revenues
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.5. Other income' AS account,
            '-' AS category,
            
            other_income.amount_token,
            token
    FROM other_income
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            other_income.amount_token,
            token
    FROM other_income
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    UNION ALL 
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.1. Protocol Capital' AS secondary_label,
            '3.1.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            CAST(fundraising.amount_token AS DOUBLE),
            token
    FROM fundraising
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            CAST(fundraising.amount_token AS DOUBLE),
            token
    FROM fundraising
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    /*
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            CASE
                WHEN tokens.address = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN tokens.address = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN tokens.address = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            0,
            tokens.address as token
    FROM calendar
    left join tokens on 1 = 1 and tokens.address NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))

UNION ALL
    
    SELECT  period,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            '1.3.1.4. Other'  AS category,
            0,
            '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj' as token
    FROM calendar
    
*/
    


) accounts

   LEFT JOIN tokens_prices ON date_trunc('day', accounts.period) = date_trunc('day', tokens_prices.period) 
    AND ( 
        (
            (accounts.token) = (tokens_prices.token) 
            AND (accounts.token) != LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
        )
        OR (
            tokens_prices.token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
            AND accounts.token  = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
        )
    )
    LEFT JOIN prices.tokens pt ON accounts.token = pt.contract_address                     
    GROUP BY 1,2,3,4,5,6,8,9, tokens_prices.decimals, tokens_prices.price, tokens_prices.token_eth_price
    ORDER BY period DESC
)

select * from accounts