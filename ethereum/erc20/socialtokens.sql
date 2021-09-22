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
\\x35bd01fc9d6d5d81ca9e055db88dc49aa2c699a8	FWB	    2	Freinds with Benefits
\\x2d94aa3e47d9d5024503ca8491fce9a2fb4da198	BANK	4	BanklessDAO
\\x35872fea6a4843facbcdbce99e3b69596a3680b8 E1337   2   E1337 Luxury Sports Apparel
\\xc22b30e4cce6b78aaaadae91e44e73593929a3e9 RAC     2   RAC Music
\\xf552b656022c218c26dad43ad88881fc04116f76 MORK    5   Hackatao art community
\\x8ba6dcc667d3ff64c1a2123ce72ff5f0199e5315 ALEX    5   Alex Masmej human IPO
\\xca3fe04c7ee111f0bbb02c328c699226acf9fd33 SEEN    2   Seen.haus nft/physical art0x35bd01fc9d6d5d81ca9e055db88dc49aa2c699a8
\\x6307b25a665efc992ec1c1bc403c38f3ddd7c661 GCR     5   Global Coin Research
\\xbcc66ed2ab491e9ae7bf8386541fb17421fa9d35 SKULL   5   Skeenee artist currency
\\x87b008e57f640d94ee44fd893f0323af933f9195 COIN    4   Social token of Marguerite DeCourcelle
\\x64d91f12ece7362f91a6f8e7940cd55f05060b92 ASH    18   Burn (ASH)
\\x7e9d8f07a64e363e97a648904a89fb4cd5fb94cd FF    18   Forefront
\\x32353a6c91143bfd6c7d363b546e62a9a2489a20 AGLD    18   Adventure Gold
\\xf1f955016ecbcd7321c7266bccfb96c68ea5e49b RLY    18   Rally


\.

COMMIT;
