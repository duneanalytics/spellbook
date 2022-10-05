CREATE OR REPLACE VIEW iearn_v2.view_ytokens (symbol, contract_address, decimals, underlying_token_address) AS VALUES
('yDAI'::text, '\x16de59092dAE5CcF4A1E6439D611fd0653f0Bd01'::bytea, 18::numeric, '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea),
('yUSDC'::text, '\xd6aD7a6750A7593E092a9B218d66C0A814a3436e'::bytea, 6::numeric, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea),
('ySUSD'::text, '\xF61718057901F84C4eEC4339EF8f0D86D2B45600'::bytea, 18::numeric, '\x57Ab1ec28D129707052df4dF418D58a2D46d5f51'::bytea),
('yUSDT'::text,'\x83f798e925BcD4017Eb265844FDDAbb448f1707D'::bytea, 6::numeric, '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea),
('yTUSD'::text,'\x73a052500105205d34Daf004eAb301916DA8190f'::bytea, 18::numeric, '\x0000000000085d4780B73119b644AE5ecd22b376'::bytea),
('yBTC'::text, '\x04Aa51bbcB46541455cCF1B8bef2ebc5d3787EC9'::bytea, 8::numeric, '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea)
;
