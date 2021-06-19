CREATE OR REPLACE VIEW dodo.view_markets (market_contract_address, base_token_symbol, quote_token_symbol, base_token_address, quote_token_address) AS VALUES
('\x75c23271661d9d143dcb617222bc4bec783eff34'::bytea, 'WETH'::text, 'USDC'::text, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea),
('\x562c0b218cc9ba06d9eb42f3aef54c54cc5a4650'::bytea, 'LINK'::text, 'USDC'::text, '\x514910771af9ca656af840dff83e8264ecf986ca'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea),
('\x0d04146b2fe5d267629a7eb341fb4388dcdbd22f'::bytea, 'COMP'::text, 'USDC'::text, '\xc00e94cb662c3520282e6f5717214004a7f26888'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea),
('\xca7b0632bd0e646b0f823927d3d2e61b00fe4d80'::bytea, 'SNX'::text, 'USDC'::text, '\xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea),
('\xc226118fcd120634400ce228d61e1538fb21755f'::bytea, 'LEND'::text, 'USDC'::text, '\x80fb784b7ed66730e8b1dbd9820afd29931aab03'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea),
('\x2109f78b46a789125598f5ad2b7f243751c2934d'::bytea, 'WBTC'::text, 'USDC'::text, '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea),
('\x1b7902a66f133d899130bf44d7d879da89913b2e'::bytea, 'YFI'::text, 'USDC'::text, '\x0bc529c00c6401aef6d220be8c6ea1667f6ad93e'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea),
('\x1a7fe5d6f0bb2d071e16bdd52c863233bbfd38e9'::bytea, 'WETH'::text, 'USDT'::text, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea),
('\xc9f93163c99695c6526b799ebca2207fdf7d61ad'::bytea, 'USDT'::text, 'USDC'::text, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea),
('\xd4a36b0acfe2931cf922ea3d91063ddfe4aff01f'::bytea, 'sUSD'::text, 'USDT'::text, '\x57ab1ec28d129707052df4df418d58a2d46d5f51'::bytea, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea),
('\x8876819535b48b551c9e97ebc07332c7482b4b2d'::bytea, 'DODO'::text, 'USDT'::text, '\x43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd'::bytea, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea),
('\x9d9793e1e18cdee6cf63818315d55244f73ec006'::bytea, 'FIN'::text, 'USDT'::text, '\x054f76beed60ab6dbeb23502178c52d6c5debe40'::bytea, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea),
('\x94512fd4fb4feb63a6c0f4bedecc4a00ee260528'::bytea, 'AAVE'::text, 'USDC'::text, '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea),
('\x85f9569b69083c3e6aeffd301bb2c65606b5d575'::bytea, 'wCRES'::text, 'USDT'::text, '\xa0afaa285ce85974c3c881256cb7f225e3a1178a'::bytea, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea),
('\x3058ef90929cb8180174d74c507176cca6835d73'::bytea, 'DAI'::text, 'USDT'::text, '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea),
('\xd84820f0e66187c4f3245e1fe5ccc40655dbacc9'::bytea, 'sUSD'::text, 'USDT'::text, '\x57ab1ec28d129707052df4df418d58a2d46d5f51'::bytea, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea)
;