{{
	config(
    schema = 'compound_v3_base',
    alias = 'ctokens',
    tags = ['static'],
    materialized = 'table'
	)
}}

select asset_symbol, comet_contract_address, collateral_token_address, asset_address
from (values
	('WETH', 0x46e6b214b524310239732D51387075E0e70970bf, 0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22, 0x4200000000000000000000000000000000000006), --cbETH
	('USDbC', 0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf, 0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22, 0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA), --cbETH
	('USDbC', 0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf, 0x4200000000000000000000000000000000000006, 0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA)  --WETH
) as x (asset_symbol, comet_contract_address, collateral_token_address, asset_address)
