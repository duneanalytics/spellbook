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
\.

COMMIT;

CREATE INDEX IF NOT EXISTS erc721_tokens_contract_address_project_name_idx ON erc721.tokens USING btree (contract_address) INCLUDE (project_name);
