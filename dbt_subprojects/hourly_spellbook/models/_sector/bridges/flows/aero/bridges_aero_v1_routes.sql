{{ config(
    schema = 'bridges_aero'
    , alias = 'v1_routes'
    , materialized = 'view'
    , filtering_columns = []
) }}

-- Aero's Metarouter is documented at https://app.aero.xyz/docs/security/.
-- These are shared USDC Warp routes, not Aero-exclusive bridges. Scale is 1:1.
-- https://github.com/hyperlane-xyz/hyperlane-registry/blob/d350acb9a6ce045758be6fa610bb9e697b7fd1bc/deployments/warp_routes/USDC/eclipsemainnet-config.yaml
SELECT blockchain, chain_id, bridge_address, token_address, mailbox_address
FROM (VALUES
    ('arbitrum', 42161, 0xad4350ee0f9f5b85bab115425426086ae8384ebb, 0xaf88d065e77c8cc2239327c5edb3a432268e5831, 0x979Ca5202784112f4738403dBec5D0F3B9daabB9),
    ('arc', 5042002, 0x56a255a080584de4433d0fe6587f4c0105715589, 0x3600000000000000000000000000000000000000, 0x7f50C5776722630a0024fAE05fDe8b47571D7B39),
    ('base', 8453, 0x37e637891a558b5b621723cbf8fc771525f280c1, 0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 0xeA87ae93Fa0019a82A727bfd3eBd1cFCa8f64f1D),
    ('ethereum', 1, 0xe1de9910fe71cc216490ac7fcf019e13a34481d7, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 0xc005dc82818d67AF737725bD4bf75435d065D239),
    ('ink', 57073, 0x0de665377a5b0d390469d0ea0fcae55e0c14f4c4, 0x2d270e6886d130d724215a266106e6832161eaed, 0x7f50C5776722630a0024fAE05fDe8b47571D7B39),
    ('optimism', 10, 0x02bfd67829317d666dc7dfa030f18eacc12c2cfb, 0x0b2c639c533813f4aa9d7837caf62653d097ff85, 0xd4C1905BB1D26BC93DAC913e13CaCC278CdCC80D)
) AS routes(blockchain, chain_id, bridge_address, token_address, mailbox_address)
