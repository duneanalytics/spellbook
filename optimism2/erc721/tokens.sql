CREATE TABLE IF NOT EXISTS erc721.tokens (
	contract_address bytea UNIQUE,
	project_name text
);

BEGIN;
DELETE FROM erc721.tokens *;


COPY erc721.tokens (contract_address, project_name) FROM stdin;
\\xb8df6cc3050cc02f967db1ee48330ba23276a492	OptiPunk
\\x52782699900df91b58ecd618e77847c5774dcd2e	Optimistic Bunnies
\\x006eb613cc586198003a119485594ecbbdf41230	OptimisticLoogies
\\x5763f564e0b5d8233da0accf2585f2dbef0f0dfa	OldEnglish (OE40)
\\xeb0d6c099b2fb18da09ad554b7612bfae6a9c9ab	Otter Coloring Book
\\x8ceac0f1fbac4cf48527b35f8bc629fb30653184	Animal Coloring Book Eraser
\\xfb0b3e0f27a2a858cc6656627e662b0d3cd5b19b	ETHDubaiTicket
\\x2596b971ee0de4532566c59fa394c0d29f21d224	OptimisticBoredApeYachtClub
\\xC7adDfaf516751e1b3C068B763bcA13dDc5499F9	MintMyRun
\\x895e1669ef3db7b31cdd6129c7f333e9db053e3d	XmasBook
\\xbE7706DFA9Cc5aEEB5b26698C1bc5c43829E808A	FancyLoogie
\\x03587c8fd5346c07bbd4e6e154e6dfcecc775236	yearn Gift NFT
\\x915d0d9e68cca951b3a0aed95f236fff912431da	Gladiators of the Galaxy
\\x7c230d7a7efbf17b2ebd2aac24a8fb5373e381b7	Pixel Bunnies
\\x3af0441ce31974c9ea6553fdba60c9cd3dfd933d	Cat Game
\\xc58c9a631ce193fC3F2Bb190Ab5Ba1BE181c09D1	Octavas
\\xA433e0Bf662Dd934833C66D4f03711e1CCE9c9B2	Ethernauts
\\xC36442b4a4522E871399CD717aBDD847Ab11FE88	Uniswap V3 Positions NFT-V1
\\x0932F43d1b5f81A34FC986f9a34D05FD5E92874b	Pegasus OG Task Ticket
\\x5dc68E29F8f8Eb9de35e655Ac541D9F5b93A31aD	BakeryNFT - 30 DAY
\\xdbfeaae58b6da8901a8a40ba0712beb2ee18368e	Dope Wars Hustlers
\\xeac4c25aaefe942d408004f5afcce947bae51484	Pegasus Welcome Ticket
\\xa698713a3bc386970Cdc95A720B5754cC0f96931	Words (viaMirror)
\\xB5604Fc106074A140DF727Fe28cd68F0dbB6C1B9	PixelConInvaders
\\x0e55e1913c50e015e0f60386ff56a4bfb00d7110	Dope Wars Gear
\\xb752dfFA1d8Fbb107cde9aa6dBB511a89A7CFf49	SpaceHook
\\x0ceAE206F543E361f5C4428da80d1E77d3D82698	SNX L222
\\x7F313258c747c19f388e4D1d219Eff26a876A743	AI Crystals
\\x5BDb639399A91890356C98Aa2De402073dc642c3	PegaPal
\\xD5f027a493848C925fD6984459F72142B42EbBBD	Connectivity
\\x4d40396b4eb19be0c1ce1b9544608068bdf6b0fc	Optimistic Apes
\\xb5abc2aa4b14e836cd202a43f53f463e6589f91b	Squibz
\\x90da6e5c645e9ebd8b1ecee565d9dd1f5ef11595	GKD Showcase 3 Poster
\\x8954397e87A891f8b23c8baA8C47f726AB00b2B1	Optimism Contributor NFTs
\\x8F05bADf192664fd3Dd48Dbc0E9aAa5E530E919D	DinoSours Collection
\\x1E13B9055CCcD45ED43EE500AFbe89D8Fc1D3B1f	NFTMapQuest
\.

COMMIT;

CREATE INDEX IF NOT EXISTS erc721_tokens_contract_address_project_name_idx ON erc721.tokens USING btree (contract_address) INCLUDE (project_name);
