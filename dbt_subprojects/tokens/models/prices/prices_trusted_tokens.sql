{{ config(
        schema='prices',
        alias = 'trusted_tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}

WITH trusted_tokens AS (
        -- Originally generated from https://dune.com/queries/3355223
        -- Maintained manually moving forward
        SELECT
                blockchain
                , contract_address
        FROM (
                VALUES
                ('arbitrum', 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8)
                , ('arbitrum', 0x82af49447d8a07e3bd95bd0d56f35241523fbab1)
                , ('arbitrum', 0xaf88d065e77c8cc2239327c5edb3a432268e5831)
                , ('arbitrum', 0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9)
                , ('arbitrum', 0x912ce59144191c1204e64559fe8253a0e49e6548)
                , ('arbitrum', 0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f)
                , ('arbitrum', 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1)
                , ('avalanche_c', 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7)
                , ('avalanche_c', 0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e)
                , ('avalanche_c', 0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7)
                , ('avalanche_c', 0xc7198437980c041c805a1edcba50c1ce5db95118)
                , ('avalanche_c', 0x152b9d0fdc40c096757f570a51e494bd4b943e50)
                , ('avalanche_c', 0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab)
                , ('avalanche_c', 0xd586e7f844cea2f87f50152665bcbc2c279d8d70)
                , ('base', 0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca)
                , ('base', 0x833589fcd6edb6e08f4c7c32d4f71b54bda02913)
                , ('base', 0x4200000000000000000000000000000000000006)
                , ('base', 0x50c5725949a6f0c72e6c4a641f24049a917db0cb)
                , ('base', 0xeb466342c4d449bc9f53a865d5cb90586f405215)
                , ('base', 0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22)
                , ('base', 0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452)
                , ('blast', 0x4300000000000000000000000000000000000004)
                , ('blast', 0x4300000000000000000000000000000000000003)
                , ('blast', 0xb1a5700fa2358173fe465e6ea4ff52e36e88e2ad)
                , ('blast', 0xf77dd21c5ce38ac08786be35ef1d1dec1a6a15f3)
                , ('bnb', 0x55d398326f99059ff775485246999027b3197955)
                , ('bnb', 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c)
                , ('bnb', 0xe9e7cea3dedca5984780bafc599bd69add087d56)
                , ('bnb', 0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d)
                , ('bnb', 0x2170ed0880ac9a755fd29b2688956bd959f933f8)
                , ('bnb', 0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c)
                , ('bnb', 0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3)
                , ('boba', 0x5DE1677344D3Cb0D7D465c10b72A8f60699C062d)
                , ('boba', 0x66a2A913e447d6b4BF33EFbec43aAeF87890FBbc)
                , ('boba', 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000)
                , ('boba', 0xa18bF3994C0Cc6E3b63ac420308E5383f53120D7)
                , ('boba', 0xf74195Bb8a5cf652411867c5C2C5b8C2a402be35)
                , ('boba', 0x7562F525106F5d54E891e005867Bf489B5988CD9)
                , ('boba', 0x68ac1623ACf9eB9F88b65B5F229fE3e2c0d5789E)
                , ('celo', 0x765de816845861e75a25fca122bb6898b8b1282a)
                , ('celo', 0x471ece3750da237f93b8e339c536989b8978a438)
                , ('celo', 0xceba9300f2b948710d2653dd7b07f33a8b32118c)
                , ('celo', 0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73)
                , ('celo', 0xd629eb00deced2a080b7ec630ef6ac117e614f1b)
                , ('celo', 0x639a647fbe20b6c8ac19e48e2de44ea792c62c5c)
                , ('celo', 0x48065fbbe25f71c9282ddf5e1cd6d6a887483d5e)
                , ('ethereum', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2)
                , ('ethereum', 0xdac17f958d2ee523a2206206994597c13d831ec7)
                , ('ethereum', 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48)
                , ('ethereum', 0x6b175474e89094c44da98b954eedeac495271d0f)
                , ('ethereum', 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599)
                , ('ethereum', 0xf939e0a03fb07f59a73314e73794be0e57ac1b4e)
                , ('ethereum', 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0) -- wstETH
                , ('fantom', 0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83)
                , ('flare', 0x1D80c49BbBCd1C0911346656B529DF9E5c2F783d)
                , ('fantom', 0x04068da6c83afcfa0e13ba15a6696662335d5b75)
                , ('fantom', 0x74b23882a30290451a17c44f4f05243b6b58c76d)
                , ('fantom', 0x8d11ec38a3eb5e956b052f67da8bdc9bef8abf3e)
                , ('fantom', 0x321162cd933e2be498cd2267a90534a804051b11)
                , ('fantom', 0x1b6382dbdea11d97f24495c9a90b7c88469134a4)
                , ('fantom', 0x28a92dde19d9989f39a49905d7c9c2fac7799bdf)
                , ('gnosis', 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d)
                , ('gnosis', 0x6a023ccd1ff6f2045c3309768ead9e68f978f6e1)
                , ('gnosis', 0xddafbb505ad214d7b80b1f830fccc89b60fb7a83)
                , ('gnosis', 0x6c76971f98945ae98dd7d4dfca8711ebea946ea6)
                , ('gnosis', 0x9c58bacc331c9aa871afd802db6379a98e80cedb)
                , ('gnosis', 0x4ecaba5870353805a9f068101a40e0f32ed605c6)
                , ('gnosis', 0x8e5bbbb09ed1ebde8674cda39a0c169401db4252)
                , ('gnosis', 0x44fa8e6f47987339850636f88629646662444217)
                , ('gnosis', 0xdd96b45877d0e8361a4ddb732da741e97f3191ff)
                , ('kaia', 0x5c13e303a62fc5dedf5b52d66873f2e59fedadc2)
                , ('kaia', 0x608792deb376cce1c9fa4d0e6b7b44f507cffa6a)
                , ('kaia', 0x19aac5f612f524b754ca7e7c41cbfa2e981a4432)
                , ('kaia', 0x98a8345bb9d3dda9d808ca1c9142a28f6b0430e1)   
                , ('kaia', 0x15d9f3ab1982b0e5a415451259994ff40369f584)
                , ('linea', 0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f)
                , ('linea', 0x176211869ca2b568f2a7d4ee941e073a821ee1ff)
                , ('linea', 0xa219439258ca9da29e9cc4ce5596924745e12b93)
                , ('linea', 0x3aab2285ddcddad8edf438c1bab47e1a9d05a9b4)
                , ('linea', 0xb5bedd42000b71fdde22d3ee8a79bd49a568fc8f)
                , ('mantle', 0x201eba5cc46d216ce6dc03f6a759e8e766e956ae)
                , ('mantle', 0xdeaddeaddeaddeaddeaddeaddeaddeaddead1111)
                , ('mantle', 0x09bc4e0d864854c6afb6eb9a9cdf58ac190d0df9)
                , ('mantle', 0xcda86a272531e8640cd7f1a92c01839911b90bb0)
                , ('mantle', 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000)
                , ('mantle', 0x371c7ec6d8039ff7933a2aa28eb827ffe1f52f07)
                , ('mantle', 0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8)
                , ('mantle', 0xeb466342c4d449bc9f53a865d5cb90586f405215)
                , ('nova', 0x750ba8b76187092B0D1E87E28daaf484d1b5273b)
                , ('nova', 0x1d05e4e72cD994cdF976181CfB0707345763564d)
                , ('nova', 0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1)
                , ('nova', 0x722E8BdD2ce80A4422E880164f2079488e115365)
                , ('nova', 0xf823C3cD3CeBE0a1fA952ba88Dc9EEf8e0Bf46AD)
                , ('optimism', 0x7f5c764cbc14f9669b88837ca1490cca17c31607)
                , ('optimism', 0x4200000000000000000000000000000000000006)
                , ('optimism', 0x94b008aa00579c1307b0ef2c499ad98a8ce58e58)
                , ('optimism', 0x0b2c639c533813f4aa9d7837caf62653d097ff85)
                , ('optimism', 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1)
                , ('optimism', 0x4200000000000000000000000000000000000042)
                , ('optimism', 0x68f180fcce6836688e9084f035309e29bf0a2095)
                , ('optimism', 0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9)
                , ('optimism', 0x1f32b1c2345538c0c6f582fcb022739c4a194ebb)
                , ('optimism', 0xdc6ff44d5d932cbd77b52e5612ba0529dc6226f1)
                , ('polygon', 0x2791bca1f2de4661ed88a30c99a7a9449aa84174)
                , ('polygon', 0xc2132d05d31c914a87c6611c10748aeb04b58e8f)
                , ('polygon', 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270)
                , ('polygon', 0x7ceb23fd6bc0add59e62ac25578270cff1b9f619)
                , ('polygon', 0x8f3cf7ad23cd3cadbd9735aff958023239c6a063)
                , ('polygon', 0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6)
                , ('polygon', 0x3c499c542cef5e3811e1192ce70d8cc03d5c3359)
                , ('ronin', 0xe514d9deb7966c8be0ca922de8a064264ea6bcd4)
                , ('ronin', 0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5)
                , ('ronin', 0x0b7007c13325c48911f73a2dad5fa5dcbf808adc)
                , ('ronin', 0x97a9107c1793bc407d6f527b77e7fff4d812bece)
                , ('ronin', 0xa8754b9fa15fc18bb59458815510e40a12cd2014)
                , ('scroll', 0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4)
                , ('scroll', 0x5300000000000000000000000000000000000004)
                , ('scroll', 0xf55bec9cafdbe8730f096aa55dad6d22d44099df)
                , ('scroll', 0x3c1bca5a656e69edcd0d4e36bebb3fcdaca60cf1)
                , ('scroll', 0xf610a9dfb7c89644979b4a0f27063e9e7d7cda32)
                , ('sei', 0xE30feDd158A2e3b13e9badaeABaFc5516e95e8C7)
                , ('sei', 0x3894085Ef7Ff0f0aeDf52E2A2704928d1Ec074F1)
                , ('sei', 0xB75D0B03c06A926e488e2659DF1A861F860bD3d1)
                , ('sei', 0x160345fC359604fC6e70E3c5fAcbdE5F7A9342d8)
                , ('solana', from_base58('So11111111111111111111111111111111111111112'))
                , ('solana', from_base58('mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So'))
                , ('solana', from_base58('Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'))
                , ('solana', from_base58('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'))
                , ('worldchain', 0x2cFc85d8E48F8EAB294be644d9E25C3030863003)
                , ('worldchain', 0x79A02482A880bCE3F13e09Da970dC34db4CD24d1)
                , ('worldchain', 0x03C7054BCB39f7b2e5B2c7AcB37583e32D70Cfa3)
                , ('worldchain', 0x4200000000000000000000000000000000000006)
                , ('worldchain', 0x859DBE24b90C9f2f7742083d3cf59cA41f55Be5d)
                , ('zkevm', 0x4f9a0e7fd2bf6067db6994cf12e4495df938e6e9)
                , ('zkevm', 0x1e4a5963abfd975d8c9021ce480b42188849d41d)
                , ('zkevm', 0x37eaa0ef3549a5bb7d431be78a3d99bd360d19e5)
                , ('zkevm', 0xa2036f0538221a77a3937f1379699f44945018d0)
                , ('zkevm', 0xb23c20efce6e24acca0cef9b7b7aa196b84ec942)
                , ('viction', 0xC054751BdBD24Ae713BA3Dc9Bd9434aBe2abc1ce)  -- WVIC
                , ('viction', 0x381B31409e4D220919B2cFF012ED94d70135A59e)  -- USDT
                , ('viction', 0x20cC4574f263C54eb7aD630c9AC6d4d9068Cf127)  -- USDC
                , ('zksync', 0x3355df6d4c9c3035724fd0e3914de96a5a83aaf4)
                , ('zksync', 0x2039bb4116b4efc145ec4f0e2ea75012d6c0f181)
                , ('zksync', 0x5aea5775959fbc2557cc8789bc1bf90a239d9a91)
                , ('zksync', 0x493257fd37edb34451f62edf8d2a0c418852ba4c)
                , ('zksync', 0x000000000000000000000000000000000000800a)
                , ('zksync', 0xbbeb516fb02a01611cbbe0453fe3c580d7281011)
                , ('zora', 0x4200000000000000000000000000000000000006)
        ) AS t (blockchain, contract_address)
), erc20 as (
        SELECT
                p.token_id
                , p.blockchain
                , p.contract_address
                , p.symbol
                , p.decimals
        FROM
                {{ ref('prices_tokens') }} AS p
        INNER JOIN
                trusted_tokens AS tt
                ON p.blockchain = tt.blockchain
                AND p.contract_address = tt.contract_address
), native_tokens AS (
        SELECT
                p.token_id
                , evm.blockchain
                , {{ var('ETH_ERC20_ADDRESS') }} as contract_address -- 0x00..00
                , p.symbol
                , 18 as decimals
        FROM
                {{ source('evms','info') }} evm
        INNER JOIN
                {{ ref('prices_native_tokens') }} p
                on evm.native_token_symbol = p.symbol
)
SELECT
        *
FROM
        erc20
UNION ALL
SELECT
        *
FROM
        native_tokens