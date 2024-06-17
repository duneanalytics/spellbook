{{ 
    config(
        schema = 'tokens_bnb'
        , alias = 'stablecoins'
        , tags = ['static']
        , materialized = 'table'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (
    VALUES
        (0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d,    'USDC', 18, 'USD Coin')
		, (0x55d398326f99059ff775485246999027b3197955,  'USDT', 18, 'Tether')
		, (0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3,  'DAI',  18, 'Dai')
		, (0xd17479997F34dd9156Deef8F95A52D81D265be9c,  'USDD', 18, 'Decentralized USD')
		, (0xe80772eaf6e2e18b651f160bc9158b2a5cafca65,  'USD+', 6,  'Overnight USD')
		, (0xb7f8cd00c5a06c0537e2abff0b58033d02e5e094,  'PAX',  18, 'Paxos Standard')
		, (0x8965349fb649a33a30cbfda057d8ec2c48abe2a2,  'USDC', 18, 'anyUSDC')
) AS temp_table (contract_address, symbol, decimals, name)
