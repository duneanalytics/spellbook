{% set blockchain = 'xrpl' %}

{{ config(
    schema = 'prices_' + blockchain,
    alias = 'tokens',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static']
    )
}}

-- ci-stamp: 1
SELECT
    token_id
    , '{{ blockchain }}' AS blockchain
    , symbol
    -- XRPL issued assets are identified by currency + issuer, not an ERC20 contract.
    , to_utf8(contract_address_native) AS contract_address
    , contract_address_native
    , cast(null AS integer) AS decimals
FROM
(
    VALUES
    ('rlusd-ripple-usd', 'RLUSD', '524C555344000000000000000000000000000000.rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De')
    , ('usdc-usdc', 'USDC', 'USDC.rGm7WCVp9gb4jZHWTEtGUr4dd74z2XuWhE')
    , ('solo-sologenic', 'SOLO', '534F4C4F00000000000000000000000000000000.rsoLo2S1kiGeCcn6hCUXVrCpGMWLrRrLZz')
    , ('csc-casinocoin', 'CSC', 'CSC.rCSCManTZ8ME9EoLrSHHYKW8PPwWMgkwr')
    , ('xrph-xrp-healthcare', 'XRPH', 'XRPH.rM8hNqA3jRJ5Zgp3Xf3xzdZcx2G37guiZk')
    , ('xah-xahau', 'XAH', 'XAH.rswh1fvyLqHizBS2awu1vs6QcmwTBd9qiv')
) AS temp (token_id, symbol, contract_address_native)
