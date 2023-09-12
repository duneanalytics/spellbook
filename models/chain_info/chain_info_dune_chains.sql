{{
  config(
    schema = 'chain_info'
    , alias = alias('dune_chains')
    , tags=['dunesql', 'static']
  )
}}


SELECT dune_name
     , chain_id
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
    ('base', 8453),
    ('celo',  42220)
) AS dune_chain_name_to_chain_id (dune_name, chain_id)
