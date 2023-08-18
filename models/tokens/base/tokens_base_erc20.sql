{{ config(
    tags=['dunesql', 'static']
    , alias = alias('erc20')
    , materialized = 'table'
    , post_hook='{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "tokens",
                                    \'["hildobby"]\') }}'
    )
}}

SELECT contract_address, symbol, decimals
FROM (VALUES
        (0x4158734D47Fc9692176B5085E0F52ee0Da5d47F1, 'BAL', 18),
        (0x30136B90e532141FeD006c61105cff3668b5c774, 'BLUE', 18),
        (0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22, 'cbETH', 18),
        (0xE2B21D4684b2bA62F3BE1FE286eacb90D26E394d, 'COC', 18),
        (0x9e1028F5F1D5eDE59748FFceE5532509976840E0, 'COMP', 18),
        (0x8Ee73c484A26e0A5df2Ee2a4960B789967dd0415, 'CRV', 18),
        (0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb, 'DAI', 18),
        (0x4621b7A9c75199271F773Ebd9A499dbd165c3191, 'DOLA', 18),
        (0xD08a2917653d4E460893203471f0000826fb4034, 'FARM', 18),
        (0x09188484e1Ab980DAeF53a9755241D759C5B7d60, 'GRG', 18),
        (0x9e5AAC1Ba1a2e6aEd6b32689DFcF62A509Ca96f3, 'HAN', 18),
        (0x3e7eF8f50246f725885102E8238CBba33F276747, 'HANeP', 18),
        (0xc5102fe9359fd9a28f877a67e36b0f050d81a3cc, 'HOP', 18),
        (0xE7798f023fC62146e8Aa1b36Da45fb70855a77Ea, 'iFARM', 18),
        (0x235226d2050C001FE78dED104364F9C2eB852E42, 'INFC', 18),
        (0x8Fbd0648971d56f1f2c35Fa075Ff5Bc75fb0e39D, 'MBS', 18),
        (0xc2106ca72996e49bBADcB836eeC52B765977fd20, 'NFTE', 18),
        (0x7C34073C56285944F9A5384137186abFAe1C3bf0, 'PLG', 18),
        (0xB6fe221Fe9EeF5aBa221c348bA20A1Bf5e73624c, 'rETH', 18),
        (0x1f73EAf55d696BFFA9b0EA16fa987B93b0f4d302, 'RPL', 18),
        (0x7D49a065D17d6d4a55dc13649901fdBB98B2AFBA, 'SUSHI', 18),
        (0x236aa50979D5f3De3Bd1Eeb40E81137F22ab794b, 'tBTC', 18),
        (0xf34e0cff046e154CAfCae502C7541b9E5FD8C249, 'THALES', 18),
        (0xA81a52B4dda010896cDd386C7fBdc5CDc835ba23, 'TRAC', 18),
        (0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA, 'USDbC', 6),
        (0x4200000000000000000000000000000000000006, 'WETH', 18),
        (0x3bB4445D30AC020a84c1b5A8A2C6248ebC9779D0, 'ZRX', 18)
     ) AS temp_table (contract_address, symbol, decimals)
