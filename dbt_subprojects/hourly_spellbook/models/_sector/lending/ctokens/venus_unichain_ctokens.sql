{{
	config(
    schema = 'venus_unichain',
    alias = 'ctokens',
    tags = ['static'],
    materialized = 'table'
	)
}}

select 
	vToken,
	vToken_contract_address,
	vToken_decimals,
	underlyingToken, 
	underlyingToken_address,
	underlyingToken_decimals,
	pool 
from (values
	('vweETH_Core', 0x0170398083eb0d0387709523bafca6426146c218, 8,  'weETH', 0x7dcc39b4d1c53cb31e1abc0e358b43987fef80f7, 18), 
	('vUNI_Core', 0x67716d6bf76170af816f5735e14c4d44d0b05ed2, 8,  'UNI', 0x8f187aa05619a017077f5308904739877ce9ea21, 18), 
	('vWBTC_Core', 0x68e2a6f7257fac2f5a557b9e83e1fe6d5b408ce5, 8,  'WBTC', 0x0555e30da8f98308edb960aa94c0db47230d2b9c, 8), 
	('vUSDC_Core', 0xb953f92b9f759d97d2f2dec10a8a3cf75fce3a95, 8,  'USDC', 0x0555e30da8f98308edb960aa94c0db47230d2b9c, 6), 
	('vwstETH_Core', 0xbec19bef402c697a7be315d3e59e5f65b89fa1bb, 8,  'wstETH', 0xc02fe7317d4eb8753a02c35fe019786854a92001, 18), 
	('vWETH_Core', 0xc219bc179c7cdb37eacb03f993f9fdc2495e3374, 8,  'WETH', 0x4200000000000000000000000000000000000006, 18), 
	('vUSD₮0_Core', 0xda7ce7ba016d266645712e2e4ebc6cc75ea8e4cd, 8,  'USD₮0', 0x9151434b16b9763660705744891fa906f660ecc5, 6) 
) as x (vToken, vToken_contract_address, vToken_decimals, underlyingToken, underlyingToken_address, underlyingToken_decimals)
