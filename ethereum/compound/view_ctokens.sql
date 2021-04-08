CREATE OR REPLACE VIEW compound.view_ctokens (symbol, contract_address, decimals, underlying_token_address) AS VALUES
   ('cBAT'::text,   '\x6c8c6b02e7b2be14d4fa6022dfd6d75921d90e4e'::bytea, 8::numeric, '\x0d8775f648430679a709e98d2b0cb6250d2887ef'::bytea),
   ('cCOMP'::text,  '\x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4'::bytea, 8::numeric, '\xc00e94cb662c3520282e6f5717214004a7f26888'::bytea),
   ('cSAI'::text,   '\xf5dce57282a584d2746faf1593d3121fcac444dc'::bytea, 8::numeric, '\x89d24a6b4ccb1b6faa2625fe562bdd9a23260359'::bytea),
   ('cETH'::text,   '\x4ddc2d193948926d02f9b1fe9e1daa0718270ed5'::bytea, 8::numeric, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea),
   ('cREP'::text,   '\x158079ee67fce2f58472a96584a73c7ab9ac95c1'::bytea, 8::numeric, '\x1985365e9f78359a9B6AD760e32412f4a445E862'::bytea),
   ('cUNI'::text,   '\x35a18000230da775cac24873d00ff85bccded550'::bytea, 8::numeric, '\x1f9840a85d5af5bf1d1762f925bdaddc4201f984'::bytea),
   ('cUSDC'::text,  '\x39aa39c021dfbae8fac545936693ac917d5e7563'::bytea, 8::numeric, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea),
   ('cWBTC'::text,  '\xc11b1268c1a384e55c48c2391d8d480264a3a7f4'::bytea, 8::numeric, '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'::bytea),
   ('cWBTC2'::text, '\xccF4429DB6322D5C611ee964527D42E5d685DD6a'::bytea, 8::numeric, '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'::bytea),
   ('cZRX'::text,   '\xb3319f5d18bc0d84dd1b4825dcde5d5f7266d407'::bytea, 8::numeric, '\xe41d2489571d322189246dafa5ebde1f4699f498'::bytea),
   ('cUSDT'::text,  '\xf650C3d88D12dB855b8bf7D11Be6C55A4e07dCC9'::bytea, 8::numeric, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea),
   ('cDAI'::text,   '\x5d3a536e4d6dbd6114cc1ead35777bab948e3643'::bytea, 8::numeric, '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea)
;   
