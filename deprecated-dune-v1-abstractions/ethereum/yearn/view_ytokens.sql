CREATE OR REPLACE VIEW yearn.view_ytokens (symbol, contract_address, decimals, underlying_token_address) AS VALUES
	('yDAI'::text, '\xC2cB1040220768554cf699b0d863A3cd4324ce32'::bytea, 18::numeric, '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea),
	('yUSDC'::text, '\x26EA744E5B887E5205727f55dFBE8685e3b21951'::bytea, 6::numeric, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea),
	('yUSDT'::text, '\xE6354ed5bC4b393a5Aad09f21c46E101e692d447'::bytea, 6::numeric, '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea),
	('yBUSD'::text, '\x04bC0Ab673d88aE9dbC9DA2380cB6B79C4BCa9aE'::bytea, 18::numeric, '\x4Fabb145d64652a948d72533023f6E7A623C7C53'::bytea)
;
