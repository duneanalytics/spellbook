{{ config(
        schema='prices_celo',
        alias='tokens',
        materialized='table',
        file_format='delta',
        tags=['static']
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
    ('celo-celo', 'celo', 'CELO', 0x471ece3750da237f93b8e339c536989b8978a438, 18), 
    ('cusd-celo-dollar', 'celo', 'cUSD', 0x765de816845861e75a25fca122bb6898b8b1282a, 18),
    ('ceur-celo-euro', 'celo', 'cEUR', 0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73, 18), --requested add to coinPaprika 2023-08-10
    --('creal-celo-brazilian-real', 'celo', 'cREAL', 0xe8537a3d056DA446677B9E9d6c5dB704EaAb4787, 18), --listed but no much volume
    ('ico-axelar', 'celo', 'AXL', 0x23ee2343B892b1BB63503a4FAbc840E0e2C6810f, 6), --requested add to coinPaprika 2023-08-10
    --('mimatic-mimatic', 'celo', 'MAI', 0xb9c8f0d3254007ee4b98970b94544e473cd610ec, 18), --requested add to coinPaprika 2023-08-10
    ('wbtc-wrapped-bitcoin', 'celo', 'WBTC', 0xd629eb00deced2a080b7ec630ef6ac117e614f1b, 18),
    ('bifi-beefyfinance', 'celo', 'BIFI', 0x639A647fbe20b6c8ac19E48E2de44ea792c62c5C, 18),
    --('ftm-fantom', 'celo', 'FTM', 0x218c3c3d49d0e7b37aff0d8bb079de36ae61a4c0, 18), --requested add to coinPaprika 2023-08-10
    --('weth-weth', 'celo', 'WETH', 0x122013fd7dF1C6F636a5bb8f03108E876548b455, 18), --requested add to coinPaprika 2023-08-10
    --('ube-ubeswap', 'celo', 'UBE', 0x00Be915B9dCf56a3CBE739D9B9c202ca692409EC, 18) --low volume
    ('wftm-wrapped-fantom', 'celo', 'WFTM', 0x218c3c3d49d0e7b37aff0d8bb079de36ae61a4c0, 18),
    ('sushi-sushi', 'celo', 'SUSHI', 0xd15ec721c2a896512ad29c671997dd68f9593226, 18),
    ('pact-impactmarket', 'celo', 'PACT', 0x46c9757c5497c5b1f2eb73ae79b6b67d119b0b58, 18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
