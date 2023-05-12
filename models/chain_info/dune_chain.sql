{{
  config(
    schema = 'chain_info',
    alias = 'dune_chains'
  )
}}


SELECT
  'chain_id' AS chain_id,
  'dune_name' AS dune_name,
FROM (
  VALUES
    ('ethereum', 1),
    ('optimism', 10),
    ('bnb', 56),
    ('gnosis', 100),
    ('polygon', 137),
    ('fantom', 250),
    ('arbitrum', 42161),
    ('avalanche_c', 43114),
    ('solana', 1399811149)
) dune_chain_name_to_chain_id  ('chain_id', 'dune_name')
