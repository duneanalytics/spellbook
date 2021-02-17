CREATE OR REPLACE VIEW perp.view_amm (symbol, base_symbol, quote_symbol, contract_address, quote_token_decimals, quote_token_address) AS VALUES
   ('ETH/USDC'::text, 'ppETH'::text, 'USDC'::text,   '\x8d22f1a9dce724d8c1b4c688d75f17a2fe2d32df'::bytea, 6::numeric, '\xddafbb505ad214d7b80b1f830fccc89b60fb7a83'::bytea),
   ('BTC/USDC'::text, 'ppBTC'::text, 'USDC'::text,   '\x0f346e19f01471c02485df1758cfd3d624e399b4'::bytea, 6::numeric, '\xddafbb505ad214d7b80b1f830fccc89b60fb7a83'::bytea),
   ('YFI/USDC'::text, 'ppYFI'::text, 'USDC'::text,   '\xd41025350582674144102b74b8248550580bb869'::bytea, 6::numeric, '\xddafbb505ad214d7b80b1f830fccc89b60fb7a83'::bytea),
   ('DOT/USDC'::text, 'ppDOT'::text, 'USDC'::text,   '\x6de775aabeeede8efdb1a257198d56a3ac18c2fd'::bytea, 6::numeric, '\xddafbb505ad214d7b80b1f830fccc89b60fb7a83'::bytea),
   ('SNX/USDC'::text, 'ppSNX'::text, 'USDC'::text,   '\xb397389b61cbf3920d297b4ea1847996eb2ac8e8'::bytea, 6::numeric, '\xddafbb505ad214d7b80b1f830fccc89b60fb7a83'::bytea),
   ('LINK/USDC'::text, 'ppLINK'::text, 'USDC'::text, '\x80daf8abd5a6ba182033b6464e3e39a0155dcc10'::bytea, 6::numeric, '\xddafbb505ad214d7b80b1f830fccc89b60fb7a83'::bytea)
;