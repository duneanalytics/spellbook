{{
	config(
		schema = 'sonne_finance_base',
		alias = 'ctokens'
	)
}}

select asset_symbol, ctoken_address, asset_address
from (values
  	('DAI', 0xb864ba2aab1f53bc3af7ae49a318202dd3fd54c2, 0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb),
  	('USDC', 0xfd68f92b45b633bbe0f475294c1a86aecd62985a, 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913),
  	('USDbC', 0x225886c9beb5eee254f79d58bbd80cf9f200d4d0, 0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA),
  	('WETH', 0x5f5c479fe590cd4442a05ae4a941dd991a633b8e, 0x4200000000000000000000000000000000000006),
  	('cbETH', 0x6c91beeceedda2089307fab818e12757948bf489, 0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22)
) as x (asset_symbol, ctoken_address, asset_address)
