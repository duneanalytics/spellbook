{{ config(
        schema='prices_celo',
        alias=alias('tokens'),
        materialized='table',
        file_format='delta',
        tags=['static','dunesql']
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
    ('axl-axelar', 'celo', 'AXL', 0x23ee2343B892b1BB63503a4FAbc840E0e2C6810f, 6), --requested add to coinPaprika 2023-08-10
    ('mimatic-mai', 'celo', 'MAI', 0xb9c8f0d3254007ee4b98970b94544e473cd610ec, 18), --requested add to coinPaprika 2023-08-10
    ('wbtc-wrapped-bitcoin', 'celo', 'WBTC', 0xBe50a3013A1c94768A1ABb78c3cB79AB28fc1aCE, 8),
    ('bifi-beefyfinance', 'celo', 'BIFI', 0x639A647fbe20b6c8ac19E48E2de44ea792c62c5C, 18)
    --('ftm-fantom', 'celo', 'FTM', 0x218c3c3d49d0e7b37aff0d8bb079de36ae61a4c0, 18), --requested add to coinPaprika 2023-08-10
    --('weth-weth', 'celo', 'WETH', 0x122013fd7dF1C6F636a5bb8f03108E876548b455, 18) --requested add to coinPaprika 2023-08-10
) as temp (token_id, blockchain, symbol, contract_address, decimals)
