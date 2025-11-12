{{ config(
    schema = 'bridges',
    alias = 'connext_chain_indexes',
    materialized = 'view',
    )
}}

-- source: https://docs.connext.network/resources/deployments

SELECT id, blockchain
    FROM (VALUES
    (6648936, 'ethereum')
    , (1869640809, 'optimism')
    , (1886350457, 'polygon')
    , (1634886255, 'arbitrum')
    , (6450786, 'bnb')
    , (6778479, 'gnosis')
    , (1818848877, 'linea')
    , (1650553709, 'base')
    , (1835365481, 'metis')
    , (1836016741, 'mode')
    , (2020368761, 'xlayer')
    , (1936027759, 'sepolia')
    , (1869640549, 'optimism_sepolia')
    , (1633842021, 'arbitrum_sepolia')
    , (2016506996, 'x1_testnet')
    ) AS x (id, blockchain)