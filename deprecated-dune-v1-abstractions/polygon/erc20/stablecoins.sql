BEGIN;

CREATE TABLE IF NOT EXISTS erc20.stablecoins (
    contract_address    bytea UNIQUE,
    symbol              text,
    decimals            numeric,
    name                text
);

DELETE FROM erc20.stablecoins *;

COPY erc20.stablecoins(contract_address, symbol, decimals, name) FROM stdin;
\\x8f3cf7ad23cd3cadbd9735aff958023239c6a063	DAI	18	(PoS) Dai Stablecoin
\\x45c32fa6df82ead1e2ef74d17b76547eddfaff89	FRAX	18	Frax
\\xa3fa99a148fa48d14ed51d610c367c61876997f1	MAI	18	miMATIC
\\x9af3b7dc29d3c4b1a5731408b6a9656fa7ac3b72	PUSD	18	PUSD
\\x2791bca1f2de4661ed88a30c99a7a9449aa84174	USDC	6	USD Coin
\\xc2132d05d31c914a87c6611c10748aeb04b58e8f	USDT	6	(PoS) Tether USD
\\xe840b73e5287865eec17d250bfb1536704b43b21	mUSD	18	mStable USD (PoS)
\\xE2Aa7db6dA1dAE97C5f5C6914d285fBfCC32A128	PAR	18	Parallel
\.

COMMIT;
