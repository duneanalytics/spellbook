CREATE OR REPLACE VIEW perp.view_amm (symbol, base_symbol, quote_symbol, contract_address, quote_token_decimals, quote_token_address) AS VALUES
   ('NRG/ETH'::text, 'NRG'::text, 'ETH'::text,   '\xa55f26319462355474a9f2c8790860776a329aa4'::bytea, 6::numeric, '\x78b050d981d7f6e019bf6e361d0d1167de6b19da'::bytea),
   ('NRG/DAI'::text, 'NRG'::text, 'DAI'::text,   '\xa55f26319462355474a9f2c8790860776a329aa4'::bytea, 6::numeric, '\x0ee5893f434017d8881750101ea2f7c49c0eb503'::bytea),
   ('NRG/SOL'::text, 'NRG'::text, 'SOL'::text,   '\xa55f26319462355474a9f2c8790860776a329aa4'::bytea, 6::numeric, '\x881145b61c604d4d27b8969b917bc4844cc8a9dc'::bytea),
   ('NRG/USDC'::text, 'NRG'::text, 'USDC'::text,   '\xa55f26319462355474a9f2c8790860776a329aa4'::bytea, 6::numeric, '\xffd7510ca0a3279c7a5f50018a26c21d5bc1dbcf'::bytea),
   ('DAI/USDC'::text, 'DAI'::text, 'USDC'::text,   '\x0ee5893f434017d8881750101ea2f7c49c0eb503'::bytea, 6::numeric, '\xffd7510ca0a3279c7a5f50018a26c21d5bc1dbcf'::bytea),
   ('ETH/DAI'::text, 'ETH'::text, 'DAI'::text, '\x78b050d981d7f6e019bf6e361d0d1167de6b19da'::bytea, 6::numeric, '\x0ee5893f434017d8881750101ea2f7c49c0eb503'::bytea)
;