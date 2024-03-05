{{
	config(
		schema = 'reactorfusion_zksync',
		alias = 'ctokens'
	)
}}

select asset_symbol, ctoken_address, asset_address
from (values
  	('USDC', 0x04e9db37d8ea0760072e1ace3f2a219988fdac29, 0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4),
  	('USDT', 0x894cccb9908a0319381c305f947ad0ef44838591, 0x493257fD37EDB34451f62EDf8D2a0C418852bA4C),
  	('BTC', 0x0a976e1e7d3052beb46085acbe1e0daccf4a19cf, 0xBBeB516fb02a01611cBBE0453Fe3c580D7281011),
  	('ETH', 0xc5db68f30d21cbe0c9eac7be5ea83468d69297e6, 0x000000000000000000000000000000000000800A)
) as x (asset_symbol, ctoken_address, asset_address)
