{{ config(
        alias = alias('erc20')
        , tags=['static', 'dunesql']
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "tokens",
                                    \'["hosuke"]\') }}'
        )
}}
SELECT
        contract_address as contract_address
        , trim(symbol) as symbol
        , decimals
FROM (VALUES
        (0x00be915b9dcf56a3cbe739d9b9c202ca692409ec, 'UBE', 18),
        (0x17700282592d6917f6a73d0bf8accf4d578c131e, 'MOO', 18),
        (0x20677d4f3d0f08e735ab512393524a3cfceb250c, 'ARI', 18),
        (0x2a3684e9dc20b857375ea04235f2f7edbe818fa7, 'USDC', 6),
        (0x4510104cf2cc3be071f171be7c47b8d6beaba234, 'CCAT', 18),
        (0x46c9757c5497c5b1f2eb73ae79b6b67d119b0b58, 'PACT', 18),
        (0x471ece3750da237f93b8e339c536989b8978a438, 'CELO', 18),
        (0x49b8990f14c0b85f528d798fc618b97bc3299c35, 'cDOGEx', 9),
        (0x5927fd244e11db1c7b1215619144d2aabac80a4f, 'cLA', 18),
        (0x635aec36c4b61bac5eb1c3eee191147d006f8a21, 'MobLP', 18),
        (0x7037f7296b2fc7908de7b57a89efaa8319f0c500, 'mCELO', 18),
        (0x73a210637f6f6b7005512677ba6b3c96bb4aa44b, 'MOBI', 18),
        (0x74c0c58b99b68cf16a717279ac2d056a34ba2bfe, 'SOURCE', 18),
        (0x765de816845861e75a25fca122bb6898b8b1282a, 'cUSD', 18),
        (0x7d00cd74ff385c955ea3d79e47bf06bd7386387d, 'mCELO', 18),
        (0x8d68062c179d2ca080c7bc3b04ab89442129daff, 'L99', 18),
        (0xa81d9a2d29373777e4082d588958678a6df5645c, 'KNX', 18),
        (0xbe50a3013a1c94768a1abb78c3cb79ab28fc1ace, 'WBTC', 8),
        (0xc16b81af351ba9e64c1a069e3ab18c244a1e3049, 'agEUR', 18),
        (0xc7a4c6ef4a16dc24634cc2a951ba5fec4398f7e0, 'MobLP', 18),
        (0xc8acba0068b0f80f5176b6e14b9c7d1af9b0f9a2, 'mooSushicUSD-USDC', 18),
        (0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73, 'cEUR', 18),
        (0xe685d21b7b0fc7a248a6a8e03b8db22d013aa2ee, 'IMMO', 9),
        (0xed6961928066d3238134933ee9cdd510ff157a6e, 'cDOGE', 18),
        (0xef4229c8c3250c675f21bcefa42f58efbff6002a, 'USDC', 6),
        (0xf3608f846ca73147f08fde8d57f45e27ceea4d61, 'cMETA', 18)
) AS temp_table (contract_address, symbol, decimals)
