{{ config(
    schema = 'uniswap_v3_ethereum',
    alias = 'tick_interval_liquidity',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "uniswap_v3",
                                \'["carlotta"]\') }}'
    )
}}

WITH pools AS (SELECT pool, fee, tickSpacing, token0, token1, te0.symbol AS symbol0, te1.symbol AS symbol1, te0.decimals AS decimals0, te1.decimals AS decimals1 
FROM FROM {{ source('uniswap_v3_ethereum', 'Factory_evt_PoolCreated') }} pl
LEFT JOIN (SELECT * FROM tokens.erc20 WHERE blockchain='ethereum') te0 ON pl.token0=te0.contract_address
LEFT JOIN (SELECT * FROM tokens.erc20 WHERE blockchain='ethereum') te1 ON pl.token1=te1.contract_address
WHERE pool IN (
lower('0xe6ff8b9a37b0fab776134636d9981aa778c4e718') --WBTC-WETH
,lower('0x4585fe77225b41b697c938b018e2ac67ac5a20c0')
,lower('0x6ab3bba2f41e7eaa262fa5a1a9b3932fa161526f')
,lower('0xcbcdf9626bc03e24f779434178a73a0b4bad62ed')
,lower('0xf4ad61db72f114be877e87d62dc5e7bd52df4d9b') --LDO-WETH
,lower('0xa3f558aebaecaf0e11ca4b2199cc5ed341edfd74')
,lower('0xcfecc1c9f3cb6190cb1ff7f65a130bfbe5107d38')
,lower('0xe7e0734ea59cfff5781d6de8d6f7a545effb91db')
,lower('0x3416cf6c708da44db2624d63ea0aaef7113527c6') --USDC-USDT
,lower('0xee4cf3b78a74affa38c6a926282bcd8b5952818d')
,lower('0xbb256c2f1b677e27118b0345fd2b3894d2e6d487')
,lower('0x7858e59e0c01ea06df3af3d20ac7b0003275d4bf')
,lower('0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640') --USDC-WETH
,lower('0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8')
,lower('0xe0554a476a092703abdb3ef35c80e0d76d32939f')
,lower('0x7bea39867e4169dbe237d55c8242a8f2fcdcc387')
,lower('0xb0cc75ed5aabb0acce7cbf0302531bb260d259c4') --SHIB-USDT
,lower('0x2dd31cc03ed996a99fbfdffa07f8f4604b1a2ec1') --GEL-WETH
,lower('0x378fabc822d9944d8bf327804745ca02aa529055')
,lower('0xf1b63cd9d80f922514c04b0fd0a30373316dd75b') --OHM-WETH
,lower('0x584ec2562b937c4ac0452184d8d83346382b5d3a')
,lower('0x88051b0eea095007d3bef21ab287be961f3d8598')
,lower('0x58557cfa7dcfe3a67e9a13080c90655e89cc1f11') --OHM-USDC
,lower('0x8406cb08a52afd2a97e958b8fad2103243b6af3e')
,lower('0x893f503fac2ee1e5b78665db23f9c94017aae97d')
,lower('0x6934290e0f75f64b83c3f473f65aefe97807103b')
))

,mint AS (SELECT pool, fee, symbol0, symbol1, decimals0, decimals1, evt_block_number AS block, amount, tickLower, tickUpper, tickSpacing, (tickUpper-tickLower)/tickSpacing AS tickintervals
FROM {{ source('uniswap_v3_ethereum', 'Pair_evt_Mint') }} a
INNER JOIN pools b
ON a.contract_address=b.pool)

,minttick AS (SELECT pool, fee, symbol0, symbol1, block, tickLower+((INCREMENT-1)*tickSpacing) AS lowerbound, tickLower+(INCREMENT*tickSpacing) AS upperbound, CAST(amount AS double) AS amount
FROM mint 
CROSS JOIN (SELECT explode(sequence(1,3000000)) AS INCREMENT)
WHERE INCREMENT<=tickintervals)

,burn AS (SELECT pool, fee, decimals0, decimals1, symbol0, symbol1, evt_block_number AS block, amount, tickLower, tickUpper, tickSpacing, (tickUpper-tickLower)/tickSpacing AS tickintervals
FROM {{ source('uniswap_v3_ethereum', 'Pair_evt_Burn') }} a
INNER JOIN pools b
ON a.contract_address=b.pool)

,burntick AS (SELECT pool, fee, symbol0, symbol1, block, tickLower+((INCREMENT-1)*tickSpacing) AS lowerbound, tickLower+(INCREMENT*tickSpacing) AS upperbound, -CAST(amount AS double) AS amount
FROM burn 
CROSS JOIN (SELECT explode(sequence(1,3000000)) AS INCREMENT)
WHERE INCREMENT<=tickintervals)

,liquidity AS (
SELECT pool, fee, symbol0, symbol1, block, lowerbound, upperbound, SUM(amount) AS liquidity
FROM (
SELECT * FROM minttick
UNION ALL
SELECT * FROM burntick
)
GROUP BY 1,2,3,4,5,6,7
)

,cumulativevalues AS (
SELECT fee, symbol0, symbol1, block, lowerbound, upperbound, liquidity
,SUM(liquidity) OVER (PARTITION BY pool, lowerbound, upperbound ORDER BY block ASC) AS cumulativeliquidity
FROM liquidity
)

,finalvalues AS (SELECT fee, symbol0, symbol1, block, lowerbound, upperbound, liquidity 
FROM cumulativevalues
)

SELECT current_timestamp() as last_updated
,symbol0, symbol1, fee/POW(10,6) AS fee, block, lowerbound, upperbound, liquidity 
FROM finalvalues