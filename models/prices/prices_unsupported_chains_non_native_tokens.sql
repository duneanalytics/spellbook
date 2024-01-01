{{ config(
        schema='prices_others',
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
    , contract_address
    , decimals
FROM
(
    VALUES
    ('btcs-ordinals', 'brc-20', 'BTCS', edc052335f914ee47a758cff988494fbb569d820e66ac8581008e44b26dcdb43i0, null),
    ('csas-ordinals', 'brc-20', 'CSAS', 08fc8bc813521635dd1471917a8d4a91749573df5454bac3b6f868e506347156i0, null),
    ('mice-ordinals', 'brc-20', 'MICE', 42dd980ad18bc5b57bb6900377b65e27cb2d7a9d5c1b993347d84c62db0dd80ei0, null),
    ('ordi-ordinals', 'brc-20', 'ORDI', b61b0172d95e266c18aea0c624db987e971a5d6d4ebc2aaed85da4642d635735i0, null),
    ('rats-ordinals', 'brc-20', 'RATS', 77df24c9f1bd1c6a606eb12eeae3e2a2db40774d54b839b5ae11f438353ddf47i0, null),
    ('sats-ordinals', 'brc-20', 'SATS', 9b664bdd6f5ed80d8d88957b63364c41f3ad4efb8eee11366aa16435974d9333i0, null),
    ('snek-snek', 'cardano', 'SNEK', 279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f, null)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
