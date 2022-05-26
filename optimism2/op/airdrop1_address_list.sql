CREATE SCHEMA IF NOT EXISTS op;

CREATE TABLE IF NOT EXISTS op.airdrop1_address_list (
	address bytea UNIQUE,
	a_is_voter numeric,
	b_is_multisig_signer numeric,
	c_is_gitcoin numeric,
	d_is_price_out numeric,
	e_op_user numeric,
	f_op_repeat numeric,
	num_categories_if_op numeric,
	g_overlap_bonus_op numeric,
	total_amount_op numeric
);

BEGIN;
DELETE FROM op.airdrop1_address_list *;


COPY op.airdrop1_address_list (address, a_is_voter,b_is_multisig_signer,c_is_gitcoin,d_is_price_out,e_op_user,f_op_repeat,num_categories_if_op,g_overlap_bonus_op,total_amount_op) FROM stdin;


\.


COMMIT;

CREATE INDEX IF NOT EXISTS op_address_idx ON erc20.tokens USING btree (address) INCLUDE (total_amount_op);
