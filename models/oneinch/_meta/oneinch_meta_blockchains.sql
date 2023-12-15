{{
    config(
        schema = 'oneinch',
        alias = 'meta_blockchains',
        materialized = 'view',
        unique_key = ['blockchain'],
        
    )
}}



with t(
    blockchain
    , chain_id
    , native_token_symbol
    , wrapped_native_token_address
    , explorer_link
    , first_deploy_at
) as (
    select * from (values 
         ('ethereum',    1,          'ETH',   0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 'https://etherscan.io/',         timestamp '2019-06-03 20:11')
        ,('bnb',         56,         'BNB',   0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c, 'https://bscscan.com/',          timestamp '2021-02-18 14:37')
        ,('polygon',     137,        'MATIC', 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270, 'https://polygonscan.com/',      timestamp '2021-05-05 09:39')
        ,('arbitrum',    42161,      'ETH',   0x82af49447d8a07e3bd95bd0d56f35241523fbab1, 'https://arbiscan.io/',          timestamp '2021-06-22 10:27')
        ,('optimism',    10,         'ETH',   0x4200000000000000000000000000000000000006, 'https://explorer.optimism.io/', timestamp '2021-11-12 09:07')
        ,('avalanche_c', 43114,      'AVAX',  0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7, 'https://snowtrace.io/',         timestamp '2021-12-22 13:18')
        ,('gnosis',      100,        'xDAI',  0xe91d153e0b41518a2ce8dd3d7944fa863463a97d, 'https://gnosisscan.io/',        timestamp '2021-12-22 13:21')
        ,('fantom',      250,        'FTM',   0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83, 'https://ftmscan.com/',          timestamp '2022-03-16 16:20')
        ,('aurora',      1313161554, 'ETH',   0xc9bdeed33cd01541e1eed10f90519d2c06fe3feb, 'https://explorer.aurora.dev/',  timestamp '2022-05-25 13:43')
        ,('klaytn',      8217,       'KLAY',  0xe4f05a66ec68b54a58b17c22107b02e0232cc817, 'https://scope.klaytn.com/',     timestamp '2022-08-02 08:37')
        ,('zksync',      324,        'ETH',   0x5aea5775959fbc2557cc8789bc1bf90a239d9a91, 'https://explorer.zksync.io/',   timestamp '2023-04-12 10:16')
        ,('base',        8453,       'ETH',   0x4200000000000000000000000000000000000006, 'https://basescan.org/',         timestamp '2023-08-08 22:19')
    )
)



-- select * from t



-- FOR TESTING
select 
    blockchain
    , chain_id
    , native_token_symbol
    , wrapped_native_token_address
    , explorer_link
    , now() - interval '7' day as  first_deploy_at -- easy dates
from t