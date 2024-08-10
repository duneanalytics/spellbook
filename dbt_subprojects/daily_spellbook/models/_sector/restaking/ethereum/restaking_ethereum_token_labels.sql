{{ config(
    schema = 'restaking_ethereum',
    alias = 'token_labels',
    tags = [ 'static'],
    unique_key = ['token_address'])
}}

select 
    'ethereum' as blockchain,
    token_address,
    token_decimals,
    token_symbol,
    token_category,
    token_project,
    lrt_project
from (
    values 
    -- lsts 
        (0xa2e3356610840701bdf5611a53974510ae27e2e1, 18, 'wBETH', 'LST', 'Binance', 'Null')
        ,(0xe95a203b1a91a908f9b9ce46459d101078c2c3cb, 18, 'ankrETH', 'LST', 'Unidentified', 'Null')
        ,(0xf1c9acdc66974dfb6decb12aa385b9cd01190e38, 18, 'osETH', 'LST', 'Unidentified', 'Null')
        ,(0x8c1bed5b9a0928467c9b1341da1d7bd5e10b6549, 18, 'LsETH', 'LST', 'Unidentified', 'Null')
        ,(0xbe9895146f7af43049ca1c1ae358b0541ea49704, 18, 'cbETH', 'LST', 'Coinbase', 'Null')
        ,(0xd5f7838f5c461feff7fe49ea5ebaf7728bb0adfa, 18, 'mETH', 'LST', 'Mantle', 'Null')
        ,(0x856c4efb76c1d1ae02e20ceb03a2a6a08b0b8dc3, 18, 'OETH', 'LST', 'Unidentified', 'Null')
        ,(0xae7ab96520de3a18e5e111b5eaab095312d7fe84, 18, 'stETH', 'LST', 'Lido', 'Null')
        ,(0xac3e018457b222d93114458476f3e3416abbe38f, 18, 'sfrxETH', 'LST', 'Frax Finance', 'Null')
        ,(0xae78736cd615f374d3085123a210448e74fc6393, 18, 'rETH', 'LST', 'Rocket Pool', 'Null')
        ,(0xf951e335afb289353dc249e82926178eac7ded78, 18, 'swETH', 'LST', 'Swell', 'Null')
        ,(0xa35b1b31ce002fbf2058d22f30f95d405200a15b, 18, 'ETHx', 'LST', 'Unidentified', 'Null')
        ,(0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0, 18, 'wstETH', 'LST', 'Lido', 'Null')
    
    -- eigenlayer lrts 
        ,(0xbf5495efe5db9ce00f80364c8b423567e58d2110, 18, 'ezETH', 'LRT', 'Renzo', 'eigenlayer')
        ,(0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee, 18, 'weETH', 'LRT', 'ether.fi', 'eigenlayer')
        ,(0x35fa164735182de50811e8e2e824cfb9b6118ac2, 18, 'eETH', 'LRT', 'ether.fi', 'eigenlayer')
        ,(0xd9a442856c234a39a81a089c06451ebaa4306a72, 18, 'pufETH', 'LRT', 'Puffer Finance', 'eigenlayer')
        ,(0xfae103dc9cf190ed75350761e95403b7b8afa6c0, 18, 'rswETH', 'LRT', 'Swell', 'eigenlayer')
        ,(0xa1290d69c65a6fe4df752f95823fae25cb99e5a7, 18, 'rsETH', 'LRT', 'Kelp DAO', 'eigenlayer')
        ,(0xf1376bcef0f78459c0ed0ba5ddce976f1ddf51f4, 18, 'uniETH', 'LRT', 'Bedrock', 'eigenlayer')
        ,(0x49446a0874197839d15395b908328a74ccc96bc0, 18, 'mstETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,(0xd05728038681bcc79b2d5aeb4d9b002e66c93a40, 18, 'mrETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,(0x8a053350ca5f9352a16ded26ab333e2d251dad7c, 18, 'mmETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,(0x879054273cb2dad631980fa4efe6d25eef08aa4, 18, 'msfrxETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,(0xe46a5e19b19711332e33f33c2db3ea143e86bc10, 18, 'mwBETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,(0x32bd822d615a3658a68b6fdd30c2fcb2c996d678, 18, 'mswETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,(0xd09124e8a1e3d620e8807ad1d968021a5495cee8, 18, 'mcbETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,(0x9a1722b1f4a1bb2f271211ade8e851afc54f77e5, 18, 'mETHx', 'LRT', 'Unidentified', 'eigenlayer')
        ,(0x5a4a503f4745c06a07e29d9a9dd88ab52f7a505b, 18, 'mAnkrETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,(0x310718274509a38cc5559a1ff48c5edbe75a382b, 18, 'mOETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,(0x352a3144e88d23427993938cfd780291d95ef091, 18, 'mOsETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,(0x6ef3d766dfe02dc4bf04aae9122eb9a0ded25615, 18, 'primeETH', 'LRT', 'Unidentified', 'eigenlayer')
        
        -- symbiotic lrts
        ,(0x917cee801a67f933f2e6b33fc0cd1ed2d5909d88, 18, 'weETHs', 'LRT', 'ether.fi', 'symbiotic')
        ,(0x5fd13359ba15a84b76f7f87568309040176167cd, 18, 'amphrETH', 'LRT', 'MEV Capital', 'symbiotic')
        ,(0xc65433845ecd16688eda196497fa9130d6c47bd8, 18, 'rsENA', 'LRT', 'Ethena', 'symbiotic')
        ,(0x8c9532a60e0e7c6bbd2b2c1303f63ace1c3e9811, 18, 'pzETH', 'LRT', 'Renzo', 'symbiotic')
        ,(0x82f5104b23ff2fa54c2345f821dac9369e9e0b26, 18, 'rsUSDe', 'LRT', 'Ethena', 'symbiotic')
        ,(0x7a4effd87c2f3c55ca251080b1343b605f327e3a, 18, 'rstETH', 'LRT', 'P2P', 'symbiotic')
        ,(0xbeef69ac7870777598a04b2bd4771c71212e6abc, 18, 'steakLRT', 'LRT', 'Steakhouse', 'symbiotic')
        ,(0x49cd586dd9ba227be9654c735a659a1db08232a9, 18, 'ifsETH', 'LRT', 'InfStones', 'symbiotic')
        ,(0x82dc3260f599f4fc4307209a1122b6eaa007163b, 18, 'LugaETH', 'LRT', 'Luganodes', 'symbiotic')
        ,(0xd6e09a5e6d719d1c881579c9c8670a210437931b, 18, 'coETH', 'LRT', 'ChorusOne', 'symbiotic')
        ,(0x84631c0d0081fde56deb72f6de77abbbf6a9f93a, 18, 'Re7LRT', 'LRT', 'Re7Labs', 'symbiotic')
        
        -- karak lrts 
        ,(0x7223442cad8e9ca474fc40109ab981608f8c4273, 18, 'weETHk', 'LRT', 'ether.fi', 'karak')
        
) as temp_table (token_address, token_decimals, token_symbol, token_category, token_project, lrt_project)

