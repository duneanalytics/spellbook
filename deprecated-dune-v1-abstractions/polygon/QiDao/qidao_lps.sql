BEGIN;

CREATE TABLE IF NOT EXISTS qidao."qidao_lps_quickswap" (
    contract_address	bytea UNIQUE,
    dex text,
    symbol_a	text,
    symbol_b	text,
    liquidation_ratio	numeric
);

DELETE FROM qidao."qidao_lps_quickswap" *;

COPY qidao."qidao_lps_quickswap"(contract_address, dex, symbol_a, symbol_b) FROM stdin;
\\x8c1b40ea78081b70f661c3286c74e71b4602c9c0	quickswap	QI	ETH
\\x9a8b2601760814019b7e6ee0052e25f1c623d1e6	quickswap	QI	MATIC
\\x25d56e2416f20de1efb1f18fd06dd12efec3d3d0	quickswap	QI	QUICK
\\x7afcf11f3e2f01e71b7cc6b8b5e707e42e6ea397	quickswap	QI	MAI
\\x160532d2536175d65c03b97b0630a9802c274dad	quickswap	MAI	USDC
\\x74214f5d8aa71b8dc921d8a963a1ba3605050781	quickswap	MAI	DAI
\\xa3fa99a148fa48d14ed51d610c367c61876997f1	quickswap	MAI	USDT
\.
COMMIT;
