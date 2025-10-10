{{ config(
    schema = 'bridges',
    alias = 'debridge_chain_indexes',
    materialized = 'view',
    )
}}

-- source: https://docs.debridge.finance/dln-details/overview/fees-supported-chains

SELECT blockchain, id
    FROM (VALUES
    ('arbitrum', 42161)
    , ('avalanche_c', 43114)
    , ('bnb', 56)
    , ('ethereum', 1)
    , ('polygon', 137)
    , ('fantom', 250)
    , ('solana', 7565164)
    , ('linea', 59144)
    , ('optimism', 10)
    , ('base', 8453)
    , ('neon', 100000001)
    , ('gnosis', 100000002)
    , ('lightlink', 100000003)
    , ('metis', 100000004)
    , ('bitrock', 100000005)
    , ('sonic', 100000014)
    , ('crossfi', 100000006)
    , ('cronos_zkevm', 100000010)
    , ('abstract', 100000017)
    , ('berachain', 100000020)
    , ('story', 100000013)
    , ('hyperevm', 100000022)
    , ('zircuit', 100000015)
    , ('flow', 100000009)
    , ('zilliqa', 100000008)
    , ('bob', 100000021)
    , ('mantle', 100000023)
    , ('plume', 100000024)
    , ('sophon', 100000025)
    , ('tron', 100000026)
    , ('sei', 100000027)
    , ('plasma', 100000028)
    ) AS x (blockchain, id)