{{
	config(
		schema = 'layer_bank_scroll',
		alias = 'ctokens'
	)
}}

select asset_symbol, ctoken_address, asset_address
from (values
	('ETH', 0x274C3795dadfEbf562932992bF241ae087e0a98C, 0x5300000000000000000000000000000000000004),
	('USDC', 0x0D8F8e271DD3f2fC58e5716d3Ff7041dBe3F0688, 0x06eFdBFf2a14a7c8E15944D1F4A48F9F95F663A4),
	('wstETH', 0xB6966083c7b68175B4BF77511608AEe9A80d2Ca4, 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32)
) as x (asset_symbol, ctoken_address, asset_address)
