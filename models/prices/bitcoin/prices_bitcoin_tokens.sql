{{ config(
        schema='prices_bitcoin',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}
SELECT 
    token_id
    , blockchain
    , symbol
    , CAST(null as VARBINARY) as contract_address
    , CAST(null as int) as decimals
FROM
(
    VALUES
    ('bear-bearordinals', 'bitcoin', 'BEAR'),
    ('btcs-btcs-ordinals', 'bitcoin', 'BTCS'),
    ('cats-catsordinals', 'bitcoin', 'CATS'),
    ('csas-csasordinals', 'bitcoin', 'CSAS'),
    ('mice-mice-ordinals', 'bitcoin', 'MICE'),
    ('ordi-ordinals', 'bitcoin', 'ORDI'),
    ('pepe-pepe-ordinals', 'bitcoin', 'PEPE'),
    ('piza-piza-ordinals', 'bitcoin', 'PIZA'),
    ('rats-ratsordinals', 'bitcoin', 'RATS'),
    ('sats-sats-ordinals', 'bitcoin', 'SATS'),
    ('trac-trac-ordinals', 'bitcoin', 'TRAC'),
    ('whee-wheeordinals', 'bitcoin', 'WHEE')
) as temp (token_id, blockchain, symbol)
