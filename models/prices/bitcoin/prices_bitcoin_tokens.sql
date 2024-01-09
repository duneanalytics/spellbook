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
    , contract_address
    , CAST(decimals as int) as decimals
FROM
(
    VALUES
    ('bear-bearordinals', 'bitcoin', 'BEAR', 'd90b041ed4414cd2c45e89270622742b68e545c2d046a023cda39ba5a6757d32i0', null),
    ('btcs-btcs-ordinals', 'bitcoin', 'BTCS', 'edc052335f914ee47a758cff988494fbb569d820e66ac8581008e44b26dcdb43i0', null),
    ('cats-catsordinals', 'bitcoin', 'CATS', '77df24c9f1bd1c6a606eb12eeae3e2a2db40774d54b839b5ae11f438353ddf47i0', null),
    ('csas-csasordinals', 'bitcoin', 'CSAS', '08fc8bc813521635dd1471917a8d4a91749573df5454bac3b6f868e506347156i0', null),
    ('mice-mice-ordinals', 'bitcoin', 'MICE', '42dd980ad18bc5b57bb6900377b65e27cb2d7a9d5c1b993347d84c62db0dd80ei0', null),
    ('ordi-ordinals', 'bitcoin', 'ORDI', 'b61b0172d95e266c18aea0c624db987e971a5d6d4ebc2aaed85da4642d635735i0', null),
    ('pepe-pepe-ordinals', 'bitcoin', 'PEPE', '54d5fe82f5d284363fec6ae6137d0e5263e237caf15211078252c0d95af8943ai0', null),
    ('piza-piza-ordinals', 'bitcoin', 'PIZA', 'c0dd0bc7d0620a02cfedc57a280cfd79823bc754623f9318d9755bcd3b131d14i0', null),
    ('rats-ratsordinals', 'bitcoin', 'RATS', '77df24c9f1bd1c6a606eb12eeae3e2a2db40774d54b839b5ae11f438353ddf47i0', null),
    ('sats-sats-ordinals', 'bitcoin', 'SATS', '9b664bdd6f5ed80d8d88957b63364c41f3ad4efb8eee11366aa16435974d9333i0', null),
    ('trac-trac-ordinals', 'bitcoin', 'TRAC', 'b006d8e232bdd01e656c40bdbec83bb38413a8af3a58570551940d8f23d4b85ai0', null),
    ('whee-wheeordinals', 'bitcoin', 'WHEE', '52030ec47450d94a4e3ca31b2e2f93d9e8998300302131a5b9e975076591adf7i0', null)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
