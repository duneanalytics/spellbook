{{
    config(
        schema = 'tokens_base'
        ,alias = 'erc20'
        ,tags=['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address
    , symbol
    , decimals
FROM
(
    VALUES
    -- tokens which do not exist in automated tokens.erc20
    (0x9e5aac1ba1a2e6aed6b32689dfcf62a509ca96f3, 'HAN', 18)
    , (0xaf19e4456c0de828496d93c7fd7eaed9b7132eeb, 'SLRS', 18)
    , (0xb6ca1a05315eb64abe842fd37b94b52934792383, 'ETHISM', 18)
    , (0x02e79d42c3297cd4154a1b52b4a3b27cd75762f1, 'FFTP', 9)
    , (0xc1bf21674a3d782ee552d835863d065b7a89d619, 'MISHA', 18)
    , (0x3e7ef8f50246f725885102e8238cbba33f276747, 'HANeP', 18)
    , (0xe2e50097c2789ae1b52e9a15603eda02517b4628, 'BAPE', 18)
    , (0xfc21540d6b89667d167d42086e1feb04da3e9b21, 'INFI', 18)
    , (0xeb585163debb1e637c6d617de3bef99347cd75c8, 'cbXEN', 18)
)
AS temp_table (contract_address, symbol, decimals)