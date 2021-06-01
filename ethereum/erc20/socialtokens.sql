CREATE TABLE IF NOT EXISTS erc20.socialtokens (
    contract_address	bytea,
    symbol	text,
    decimals numeric,
    name	text
);

BEGIN;
DELETE FROM erc20.socialtokens *;

COPY erc20.socialtokens(contract_address, symbol, decimals, name) FROM stdin;
\\x9355372396e3f6daf13359b7b607a3374cc638e0	WHALE	2	Whale 
\\xfb5453340c03db5ade474b27e68b6a9c6b2823eb	ROBOT	2	MetaFactory 
\\x35bd01fc9d6d5d81ca9e055db88dc49aa2c699a8	FWB	  2	Freinds with Benefits
\\x2d94aa3e47d9d5024503ca8491fce9a2fb4da198	BANK	4	BanklessDAO

\.

COMMIT;
