{{
  config(
    schema = 'chain_info'
    , alias = 'dune_chains'
    , tags=['static']
  )
}}


SELECT dune_name
     , chain_name
     , chain_id
     , chain_type
FROM (
  VALUES
    ('ethereum',	'Ethereum Mainnet',	1,	'Mainnet'),
    ('optimism',	'OP Mainnet',	10,		'Mainnet'),
    ('bnb',		'BNB Chain',	56,		'Mainnet'),
    ('gnosis',		'Gnosis Chain',	100,		'Mainnet'),
    ('polygon',		'Polygon Mainnet',	137,		'Mainnet'),
    ('fantom',		'Fantom Mainnet',	250,	'Mainnet'),
    ('arbitrum',	'Arbitrum One',	42161,		'Mainnet'),
    ('avalanche_c',	'Avalanche C-Chain',	43114,	'Mainnet'),
    ('celo',		'Celo Mainnet',	42220,	'Mainnet'),

    ('goerli',		'Ethereum Goerli',	5,	'Testnet')

) AS dune_chain_name_to_chain_id (dune_name, chain_name, chain_id, chain_type)
