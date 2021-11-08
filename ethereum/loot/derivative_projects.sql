CREATE TABLE IF NOT EXISTS loot.derivative_projects (
	contract_address bytea UNIQUE,
	project_name text
);

BEGIN;
DELETE FROM loot.derivative_projects *;

COPY loot.derivative_projects (contract_address, project_name) FROM stdin;
\\x42A87e04f87A038774fb39c0A61681e7e859937b	ability score
\\x1CA5694E2bAa4A4D1Fc95f3cb01d8A940b336908	abodes
\\xcc56775606730c96ea245d9cf3890247f1c57fb1	abstract loot
\\x615a610649e656485d9baf0ebe525496d7b78e24	banquets
\\x4deaFaA0f5512AFa1F7fA74fD83Cb98C498f3D7A	character
\\x4335541d17f6344c29f2412e520ed71639150ead	emoji loot
\\xc32a49ffbb4cd071c8bfd335250cf223890b4dfd	encounters
\\xB8AF61Bf2C0D8D4F65ebeCb4f46124AbDD462699	foes
\\xb79d2717D14741266E1c23ff67Dcb936e792113b	food supplies
\\xF22d64F31B312a64C900Dd6fB30b31711c463132	summoners
\\xed92dbe9df63728f5e92a2b8f2bc617082ee760b	loot army
\\x6A364afB113A46DC67DD659cE67f4b518b4c9D14	loot avatars
\\x7403AC30DE7309a0bF019cdA8EeC034a5507cbB3	characters
\\xcCaB950f5B192603a94a26c4Fa00C8D2d392B98d	loot class
\\x3461D89c7DD0119c6411850eb69a8A0a5531DAe4	companion
\\x54c52CA173742F309d95c820064690034caD78Ae	loot conditions
\\x37A8FAAc244B4D21BAde30DB8AccEE784A3f170D	loot for ape
\\x13a48f723f4AD29b6da6e7215Fe53172C027d98f	loot for cyberpunks
\\xFECc1E1449496c0CDdFb1A075E0Ef74C50538c1A	LootDicc
\\x32E58C6F1FF983924F385cE6aadF0595577beda1	Lore
\\x7AbCAb68ef30050D1eEeD3a6554587a1DC9E26EB	loot of Ether
\\x3b1bB53b1A42ff61B7399Fc196469A742cd3E98D	loot personality
\\x511372B44231a31527025a3D273C1dc0a83D77aF	maps restored
\\xAA9c2198CC110a875BE536896522bAd2B5a9856f	monsters
\\x4e8234D076caEb90604a7E5b6A584ee4eb18490a	mounts
\\xb9310aF43F4763003F42661f6FC098428469aDAB	name
\\x70F11Bc4d6C07C821b20bbE1872c35aB57F0a112	Familiars 
\\x15e32baC6C5f89C66631F3a8391bc49EACc03985	Planets with loot
\\x38f1e0fb6c88209794c1b42374e45e18e7303cb3	potions
\\x4de9d18Fd8390c12465bA3C6cc8032992fD7655d	quests
\\x7AFe30cB3E53dba6801aa0EA647A0EcEA7cBe18d	realms
\\x60F88993099b6c94b553Ed7a1ee11cb63278F929	lootspell
\\x38e942948Cea825992F105e0EC4A2ee9138AFaE4	wizardspell
\\xdeed638915ea9160912a77221829364f19a7b99d	Super Loot
\\xF7AC82FedA08d0f3E071847250521c1297E1aF9c	Travelling musician
\\xf3DFbE887D81C442557f7a59e3a0aEcf5e39F6aa	Treasure
\\x5477181b78d9C882d13cac80098Ac942547C79BE	LootRock
\\xeC43a2546625c4C82D905503bc83e66262f0EF84	LootRock (for adventurers)
\\xeCb9B2EA457740fBDe58c758E4C574834224413e	MonsterMaps (Monster)
\\x6C8715ade6361D35c941EB901408EFca8A20F65a	MonsterMaps (Maps)
\\x76E3dea18e33e61DE15a7d17D9Ea23dC6118e10f	Doggos (for DOG Owners)
\\x83f1d1396b19fed8fbb31ed189579d07362d661d	Hymns (For Adventurers)
\\x39c29999Cc4F8b8536b44CDA91f636C44ef054D3	Loot Tunes
\\x0c07150e08e5deCfDE148E7dA5A667043F579AFC	Mudverse
\\xf4b6040a4b1b30f1d1691699a8f3bf957b03e463	Genesis Mana
\\xb9a9F29C037d09D284e6e218519c7BB67fBD2ef8	Familiars (for Adventurers)
\\x5bf553e149d2e0d4725cebee5ec18ba49b6ffc33	Loot Character
\\x0ac0ecc6d249f1383c5c7c2ff4941bd56decdd14	Loot Weapon
\\x9b51a88cffe9b50e043661ddd7f492cc3888fcbf	Lootmart Items
\\x71355f4a94f46ee32eb443ad2bde2dec0470f949	Lootmart Adventurers
\.

COMMIT;

CREATE INDEX IF NOT EXISTS loot_derivative_project_contract_address ON loot.derivative_projects USING btree (contract_address);
CREATE INDEX IF NOT EXISTS loot_derivative_project_name ON loot.derivative_projects USING btree (project_name);
