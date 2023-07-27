{{ config(
	tags=['legacy'],
	
    schema = 'compound_v2_ethereum',
    alias = alias('ctokens', legacy_model=True),
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "compound_v2",
                                    \'["bizzyvinci"]\') }}'
)}}

select asset_symbol, ctoken_address, asset_address, decimals_mantissa
from (values
  	('AAVE', '0xe65cdb6479bac1e22340e4e755fae7e509ecd06c', '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', 1e18),
  	('BAT', '0x6c8c6b02e7b2be14d4fa6022dfd6d75921d90e4e', '0x0d8775f648430679a709e98d2b0cb6250d2887ef', 1e18),
  	('COMP', '0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4', '0xc00e94cb662c3520282e6f5717214004a7f26888', 1e18),
  	('DAI', '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643', '0x6b175474e89094c44da98b954eedeac495271d0f', 1e18),
    -- cETH doesn't have underlying asset, but WETH is chosen because of prices.usd
  	('WETH', '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 1e18),
  	('FEI', '0x7713dd9ca933848f6819f38b8352d9a15ea73f67', '0x956f47f50a910163d8bf957cf5846d573e7f87ca', 1e18),
  	('LINK', '0xface851a4921ce59e912d19329929ce6da6eb0c7', '0x514910771af9ca656af840dff83e8264ecf986ca', 1e18),
  	('MKR', '0x95b4ef2869ebd94beb4eee400a99824bf5dc325b', '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2', 1e18),
  	('REP', '0x158079ee67fce2f58472a96584a73c7ab9ac95c1', '0x1985365e9f78359a9b6ad760e32412f4a445e862', 1e18),
  	('SAI', '0xf5dce57282a584d2746faf1593d3121fcac444dc', '0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359', 1e18),
  	('SUSHI', '0x4b0181102a0112a2ef11abee5563bb4a3176c9d7', '0x6b3595068778dd592e39a122f4f5a5cf09c90fe2', 1e18),
  	('TUSD', '0x12392f67bdf24fae0af363c24ac620a2f67dad86', '0x0000000000085d4780b73119b644ae5ecd22b376', 1e18),
  	('UNI', '0x35a18000230da775cac24873d00ff85bccded550', '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984', 1e18),
  	('USDC', '0x39aa39c021dfbae8fac545936693ac917d5e7563', '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 1e6),
  	('USDP', '0x041171993284df560249b57358f931d9eb7b925d', '0x8e870d67f660d95d5be530380d0ec0bd388289e1', 1e18),
  	('USDT', '0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9', '0xdac17f958d2ee523a2206206994597c13d831ec7', 1e6),
  	('WBTC', '0xc11b1268c1a384e55c48c2391d8d480264a3a7f4', '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 1e8),
  	('WBTC', '0xccf4429db6322d5c611ee964527d42e5d685dd6a', '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 1e8),
  	('YFI', '0x80a2ae356fc9ef4305676f7a3e2ed04e12c33946', '0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e', 1e18),
  	('ZRX', '0xb3319f5d18bc0d84dd1b4825dcde5d5f7266d407', '0xe41d2489571d322189246dafa5ebde1f4699f498', 1e18)
) as x (asset_symbol, ctoken_address, asset_address, decimals_mantissa)