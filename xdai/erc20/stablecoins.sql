CREATE TABLE IF NOT EXISTS erc20.stablecoins
(
    contract_address bytea PRIMARY KEY,
    symbol           text,
    decimals         numeric
);

BEGIN;
DELETE
FROM erc20.stablecoins *;

COPY erc20.stablecoins (contract_address, symbol, decimals) FROM stdin;
\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee	xDAI	18
\\x0905ab807f8fd040255f0cf8fa14756c1d824931	xOWL	18
\\xe91d153e0b41518a2ce8dd3d7944fa863463a97d	WXDAI	18
\\x4ecaba5870353805a9f068101a40e0f32ed605c6	USDT	6
\\xddafbb505ad214d7b80b1f830fccc89b60fb7a83	USDC	6
\\x44fa8e6f47987339850636f88629646662444217	DAI	18
\.

COMMIT;
