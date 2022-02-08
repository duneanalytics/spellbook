CREATE OR REPLACE VIEW compound.view_ctokens (symbol, contract_address, decimals, underlying_token_address) AS VALUES
   ('vBNB'::text,   '\xa07c5b74c9b40447a954e1466938b865b6bbea36'::bytea, 8::numeric, '\xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c'::bytea),
   ('vUSDT'::text,  '\xfD5840Cd36d94D7229439859C0112a4185BC0255'::bytea, 8::numeric, '\x55d398326f99059fF775485246999027B3197955'::bytea),
   ('vUSDC'::text,  '\xecA88125a5ADbe82614ffC12D0DB554E2e2867C8'::bytea, 8::numeric, '\x8ac76a51cc950d9822d68b83fe1ad97b32cd580d'::bytea),
   ('vBTC'::text,   '\x882c173bc7ff3b7786ca16dfed3dfffb9ee7847b'::bytea, 8::numeric, '\x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c'::bytea),
   ('vXVS'::text,   '\x151b1e2635a717bcdc836ecd6fbb62b674fe3e1d'::bytea, 8::numeric, '\xcf6bb5389c92bdda8a3747ddb454cb7a64626c63'::bytea),
;   
