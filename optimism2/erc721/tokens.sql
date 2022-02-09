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
\.

COMMIT;

CREATE INDEX IF NOT EXISTS erc721_tokens_contract_address_project_name_idx ON erc721.tokens USING btree (contract_address) INCLUDE (project_name);
