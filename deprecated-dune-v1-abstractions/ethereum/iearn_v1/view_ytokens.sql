CREATE OR REPLACE VIEW iearn_v1.view_ytokens (symbol, contract_address, decimals, underlying_token_address) AS VALUES
('iETH'::text, '\x9Dde7cdd09dbed542fC422d18d89A589fA9fD4C0'::bytea, 18::numeric, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), -- note: using weth contract address here for convenience
('yDAI'::text, '\x9D25057e62939D3408406975aD75Ffe834DA4cDd'::bytea, 18::numeric, '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea),
('yUSDC'::text, '\xa2609b2b43ac0f5ebe27deb944d2a399c201e3da'::bytea, 6::numeric, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea),
('ySUSD'::text, '\x36324b8168f960A12a8fD01406C9C78143d41380'::bytea, 18::numeric, '\x57Ab1ec28D129707052df4dF418D58a2D46d5f51'::bytea),
('yUSDT'::text,'\xa1787206d5b1bE0f432C4c4f96Dc4D1257A1Dd14'::bytea, 6::numeric, '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea),
('yCRV'::text,'\x9Ce551A9D2B1A4Ec0cc6eB0E0CC12977F6ED306C'::bytea, 18::numeric, '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea),
('yBTC'::text, '\x04EF8121aD039ff41d10029c91EA1694432514e9'::bytea, 8::numeric, '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea)
;
