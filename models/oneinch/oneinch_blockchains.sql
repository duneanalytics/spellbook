{{
    config(
        schema = 'oneinch',
        alias = alias('blockchains'),
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['id'],
        tags = ['dunesql']
    )
}}

with

    blockchains(blockchain, id, explorer, token) as (values
          ('ethereum'       , 1             , 'https://etherscan.io/'               , 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2) -- WETH
        , ('bnb'            , 56            , 'https://bscscan.com/'                , 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c) -- BNB
        , ('polygon'        , 137           , 'https://polygonscan.com/'            , 0x0000000000000000000000000000000000001010) -- MATIC
        , ('arbitrum'       , 42161         , 'https://arbiscan.io/'                , 0x82af49447d8a07e3bd95bd0d56f35241523fbab1) -- WETH
        , ('gnosis'         , 100           , 'https://gnosisscan.io/'              , 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d) -- XDAI
        , ('avalanche_c'    , 43114         , 'https://avascan.info/'               , 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7) -- AVAX
        , ('optimism'       , 10            , 'https://optimistic.etherscan.io/'    , 0x4200000000000000000000000000000000000006) -- WETH
        , ('fantom'         , 250           , 'https://ftmscan.com/'                , 0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83) -- WFTM
        , ('aurora'         , 1313161554    , 'https://explorer.aurora.dev/'        , 0xC9BdeEd33CD01541e1eeD10f90519d2C06Fe3feB) -- WETH
        , ('klaytn'         , 8217          , 'https://scope.klaytn.com/'           , 0xe4f05a66ec68b54a58b17c22107b02e0232cc817) -- WKLAY
        , ('base'           , 8453          , 'https://basescan.org/'               , 0x4200000000000000000000000000000000000006) -- WETH
        , ('zksync'         , 324           , 'https://explorer.zksync.io/'         , 0x5aea5775959fbc2557cc8789bc1bf90a239d9a91) -- WETH
    )

select
      coalesce(info.blockchain, info.blockchain) as blockchain
    , coalesce(chain_id, id) as id
    , coalesce(explorer_link, explorer) as explorer
    , coalesce(wrapped_native_token_address, token) as wrapped_native_token_address
    , chain_type
    , native_token_symbol
    , first_block_time
    , name
from blockchains
full join evms.info on info.chain_id = blockchains.id