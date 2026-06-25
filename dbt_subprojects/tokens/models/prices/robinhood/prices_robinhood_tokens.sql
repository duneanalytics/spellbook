{% set blockchain = 'robinhood' %}

{{ config(
    schema = 'prices_' + blockchain,
    alias = 'tokens',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static']
    )
}}

-- Draft seed from top erc20_robinhood.evt_transfer contracts. Token metadata
-- is unconfirmed until tokens.erc20 has Robinhood coverage or the integration
-- form provides canonical symbols/decimals/CoinPaprika ids.
SELECT
    token_id
    , '{{ blockchain }}' as blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES
    (cast(NULL as varchar), 'UNKNOWN', 0x0bd7d308f8e1639fab988df18a8011f41eacad73, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0x01637b14b7378b99de75a64d50656d98488d9a4d, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0x5fc5360d0400a0fd4f2af552add042d716f1d168, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0x020bfc650a365f8bb26819deaabf3e21291018b4, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0xf3ffa92a7bea2c781c879bc02ab16bb90580cbf8, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0xa15e8847389baf535307b1c185429b6b80296c21, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0xbf72347bacefe747eaf48b8a66e38babad3020a0, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0xf8c0e9b26971c5df9b754e5e0f5ad78c35770000, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0xeb69d27d98dc51c246b29ad1ac07bdabb25bbfd0, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0x0a31bb991bf61e24d82009e05792a6a6547468f9, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0xd7321801caae694090694ff55a9323139f043b88, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0x8d4dfaaa4198b6486e0293fec914c2b6a821d4dc, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0x0b52abc0edf8252ac136afa31a0ca797b86fe8c5, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0x7e86381a763f0ecca2bdf27c54eac403ddd48123, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0xdca6da2adcebe397c15f348f6ee38ee0ebe6b867, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0xd19e64e8ec2354305efb3d8f599a3a39c28253fa, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0xa563a49b2dedb1ea714598bbfc7c776b3b8e47ad, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0x2ccfb7137d58945e198ea4ef926e9f3cbfc1f5cc, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0x6cd2628bf8da546b76b1c4a43ce49f25d3140d2a, cast(NULL as integer))
    , (cast(NULL as varchar), 'UNKNOWN', 0x938f528c2edb3f380cf50913093610b479370e41, cast(NULL as integer))
) as temp (token_id, symbol, contract_address, decimals)
