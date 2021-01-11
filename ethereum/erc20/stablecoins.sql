CREATE TABLE IF NOT EXISTS erc20.stablecoins (
    contract_address	bytea,
    symbol	text,
    decimals numeric,
    name	text
);

BEGIN;
DELETE FROM erc20.stablecoins *;

COPY erc20.stablecoins(contract_address, symbol, decimals, name) FROM stdin;
\\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48	USDC	6	USD Circle
\\x6b175474e89094c44da98b954eedeac495271d0f	DAI	18	DAI Maker DAO
\\x4fabb145d64652a948d72533023f6e7a623c7c53	BUSD	18	Binance USD
\\x8e870d67f660d95d5be530380d0ec0bd388289e1	PAX	18	Paxos Standard
\\x0000000000085d4780b73119b644ae5ecd22b376	TUSD	18	TrueUSD
\\xdf574c24545e5ffecb9a659c229253d4111d87e1	HUSD	8	St Coins
\\x57Ab1ec28D129707052df4dF418D58a2D46d5f51	sUSD	18	Synthetix sUSD
\\x57Ab1E02fEE23774580C119740129eAC7081e9D3	sUSD	18	Synthetix sUSD
\\x056fd409e1d7a124bd7017459dfea2f387b6d5cd	GUSD	2	Gemini dollar
\\xdac17f958d2ee523a2206206994597c13d831ec7	USDT	6	Tether
\.

COMMIT;
