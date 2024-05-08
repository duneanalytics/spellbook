{% macro oneinch_blockchain_macro(blockchain) %}

{% set
    config = {
        "ethereum":      "1,            'ETH',     0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 'https://etherscan.io',         timestamp '2019-06-03 20:11', array[0xa88800cd213da5ae406ce248380802bd53b47647]",
        "bnb":           "56,           'BNB',     0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c, 'https://bscscan.com',          timestamp '2021-02-18 14:37', array[0x1d0ae300eec4093cee4367c00b228d10a5c7ac63]",
        "polygon":       "137,          'MATIC',   0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270, 'https://polygonscan.com',      timestamp '2021-05-05 09:39', array[0x1e8ae092651e7b14e4d0f93611267c5be19b8b9f]",
        "arbitrum":      "42161,        'ETH',     0x82af49447d8a07e3bd95bd0d56f35241523fbab1, 'https://arbiscan.io',          timestamp '2021-06-22 10:27', array[0x4bc3e539aaa5b18a82f6cd88dc9ab0e113c63377]",
        "optimism":      "10,           'ETH',     0x4200000000000000000000000000000000000006, 'https://explorer.optimism.io', timestamp '2021-11-12 09:07', array[0xd89adc20c400b6c45086a7f6ab2dca19745b89c2]",
        "avalanche_c":   "43114,        'AVAX',    0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7, 'https://snowtrace.io',         timestamp '2021-12-22 13:18', array[0x7731f8df999a9441ae10519617c24568dc82f697]",
        "gnosis":        "100,          'xDAI',    0xe91d153e0b41518a2ce8dd3d7944fa863463a97d, 'https://gnosisscan.io',        timestamp '2021-12-22 13:21', array[0xcbdb7490968d4dbf183c60fc899c2e9fbd445308]",
        "fantom":        "250,          'FTM',     0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83, 'https://ftmscan.com',          timestamp '2022-03-16 16:20', array[0xa218543cc21ee9388fa1e509f950fd127ca82155]",
        "base":          "8453,         'ETH',     0x4200000000000000000000000000000000000006, 'https://basescan.org',         timestamp '2023-08-08 22:19', array[0x7f069df72b7a39bce9806e3afaf579e54d8cf2b9]",
        "zksync":        "324,          'ETH',     0x5aea5775959fbc2557cc8789bc1bf90a239d9a91, 'https://explorer.zksync.io',   timestamp '2023-04-12 10:16', array[0x11de482747d1b39e599f120d526af512dd1a9326]",
        "aurora":        "1313161554,   'ETH',     0xc9bdeed33cd01541e1eed10f90519d2c06fe3feb, 'https://explorer.aurora.dev',  timestamp '2022-05-25 16:14', array[0xd41b24bba51fac0e4827b6f94c0d6ddeb183cd64]",
        "klaytn":        "8217,         'ETH',     0x,                                         'https://klaytnscope.com',      timestamp '2022-08-02 09:39', array[0xa218543cc21ee9388fa1e509f950fd127ca82155]",
    }
%}

{% set column_names =
        "blockchain,     chain_id,      native_token_symbol, wrapped_native_token_address,     explorer_link,                  first_deploy_at,              fusion_settlement_addresses"
%}

select * from (values
    ('{{ blockchain }}', {{ config[blockchain] }})
) as t({{ column_names }})

{% endmacro %}