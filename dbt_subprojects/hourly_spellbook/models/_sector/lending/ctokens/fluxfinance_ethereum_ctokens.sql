{{
	config(
		schema = 'fluxfinance_ethereum',
		alias = 'ctokens'
	)
}}

select asset_symbol, ctoken_address, asset_address
from (values
  	('USDC', 0x465a5a630482f3abd6d3b84b39b29b07214d19e5, 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48),
  	('DAI', 0xe2ba8693ce7474900a045757fe0efca900f6530b, 0x6B175474E89094C44Da98b954EedeAC495271d0F),
  	('USDT', 0x81994b9607e06ab3d5cf3afff9a67374f05f27d7, 0xdAC17F958D2ee523a2206206994597C13D831ec7),
  	('FRAX', 0x1c9a2d6b33b4826757273d47ebee0e2dddcd978b, 0x853d955aCEf822Db058eb8505911ED77F175b99e),
  	('OUSG', 0x1dD7950c266fB1be96180a8FDb0591F70200E018, 0x1B19C19393e2d034D8Ff31ff34c81252FcBbee92)
) as x (asset_symbol, ctoken_address, asset_address)
