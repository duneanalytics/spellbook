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
    VALUES 
    -- lsts 
        ('0xa2e3356610840701bdf5611a53974510ae27e2e1', 18, 'wBETH', 'LST', 'Binance', 'Null')
        ,('0xa2E3356610840701BDf5611a53974510Ae27E2e1', 18, 'wBETH', 'LST', 'Binance', 'Null')
        ,('0xe95a203b1a91a908f9b9ce46459d101078c2c3cb', 18, 'ankrETH', 'LST', 'Unidentified', 'Null')
        ,('0xf1c9acdc66974dfb6decb12aa385b9cd01190e38', 18, 'osETH', 'LST', 'Unidentified', 'Null')
        ,('0x8c1bed5b9a0928467c9b1341da1d7bd5e10b6549', 18, 'LsETH', 'LST', 'Unidentified', 'Null')
        ,('0xbe9895146f7af43049ca1c1ae358b0541ea49704', 18, 'cbETH', 'LST', 'Coinbase', 'Null')
        ,('0xd5f7838f5c461feff7fe49ea5ebaf7728bb0adfa', 18, 'mETH', 'LST', 'Mantle', 'Null')
        ,('0x856c4efb76c1d1ae02e20ceb03a2a6a08b0b8dc3', 18, 'OETH', 'LST', 'Unidentified', 'Null')
        ,('0xae7ab96520de3a18e5e111b5eaab095312d7fe84', 18, 'stETH', 'LST', 'Lido', 'Null')
        ,('0xac3e018457b222d93114458476f3e3416abbe38f', 18, 'sfrxETH', 'LST', 'Frax Finance', 'Null')
        ,('0xae78736cd615f374d3085123a210448e74fc6393', 18, 'rETH', 'LST', 'Rocket Pool', 'Null')
        ,('0xf951e335afb289353dc249e82926178eac7ded78', 18, 'swETH', 'LST', 'Swell', 'Null')
        ,('0xa35b1b31ce002fbf2058d22f30f95d405200a15b', 18, 'ETHx', 'LST', 'Unidentified', 'Null')
        ,('0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0', 18, 'wstETH', 'LST', 'Lido', 'Null')
    
    -- eigenlayer lrts 
        ,('0xbf5495Efe5DB9ce00f80364C8B423567e58d2110', 18, 'ezETH', 'LRT', 'Renzo', 'eigenlayer')
        ,('0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee', 18, 'weETH', 'LRT', 'ether.fi', 'eigenlayer')
        ,('0x35fA164735182de50811E8e2E824cFb9B6118ac2', 18, 'eETH', 'LRT', 'ether.fi', 'eigenlayer')
        ,('0xD9A442856C234a39a81a089C06451EBAa4306a72', 18, 'pufETH', 'LRT', 'Puffer Finance', 'eigenlayer')
        ,('0xFAe103DC9cf190eD75350761e95403b7b8aFa6c0', 18, 'rswETH', 'LRT', 'Swell', 'eigenlayer')
        ,('0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7', 18, 'rsETH', 'LRT', 'Kelp DAO', 'eigenlayer')
        ,('0xF1376bceF0f78459C0Ed0ba5ddce976F1ddF51F4', 18, 'uniETH', 'LRT', 'Bedrock', 'eigenlayer')
        ,('0x49446A0874197839D15395B908328a74ccc96Bc0', 18, 'mstETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,('0xd05728038681bcc79b2d5aeb4d9b002e66C93A40', 18, 'mrETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,('0x8a053350ca5F9352a16deD26ab333e2D251DAd7c', 18, 'mmETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,('0x879054273cb2DAD631980Fa4efE6d25eeFe08AA4', 18, 'msfrxETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,('0xE46a5E19B19711332e33F33c2DB3eA143e86Bc10', 18, 'mwBETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,('0x32bd822d615A3658A68b6fDD30c2fcb2C996D678', 18, 'mswETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,('0xD09124e8a1e3D620E8807aD1d968021A5495CEe8', 18, 'mcbETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,('0x9a1722b1f4A1BB2F271211ade8e851aFc54F77E5', 18, 'mETHx', 'LRT', 'Unidentified', 'eigenlayer')
        ,('0x5A4A503F4745c06A07E29D9a9DD88aB52f7a505B', 18, 'mAnkrETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,('0x310718274509a38cc5559a1ff48c5eDbE75a382B', 18, 'mOETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,('0x352a3144e88D23427993938cfd780291D95eF091', 18, 'mOsETH', 'LRT', 'Unidentified', 'eigenlayer')
        ,('0x6ef3D766Dfe02Dc4bF04aAe9122EB9A0Ded25615', 18, 'primeETH', 'LRT', 'Unidentified', 'eigenlayer')
        
        -- symbiotic lrts
        ,('0x917ceE801a67f933F2e6b33fC0cD1ED2d5909D88', 18, 'weETHs', 'LRT', 'ether.fi', 'symbiotic')
        ,('0x5fd13359ba15a84b76f7f87568309040176167cd', 18, 'amphrETH', 'LRT', 'MEV Capital', 'symbiotic')
        ,('0xc65433845ecd16688eda196497fa9130d6c47bd8', 18, 'rsENA', 'LRT', 'Ethena', 'symbiotic')
        ,('0x8c9532a60e0e7c6bbd2b2c1303f63ace1c3e9811', 18, 'pzETH', 'LRT', 'Renzo', 'symbiotic')
        ,('0x82f5104b23ff2fa54c2345f821dac9369e9e0b26', 18, 'rsUSDe', 'LRT', 'Ethena', 'symbiotic')
        ,('0x7a4EffD87C2f3C55CA251080b1343b605f327E3a', 18, 'rstETH', 'LRT', 'P2P', 'symbiotic')
        ,('0xbeef69ac7870777598a04b2bd4771c71212e6abc', 18, 'steakLRT', 'LRT', 'Steakhouse', 'symbiotic')
        ,('0x49cd586dd9ba227be9654c735a659a1db08232a9', 18, 'ifsETH', 'LRT', 'InfStones', 'symbiotic')
        ,('0x82dc3260f599f4fc4307209a1122b6eaa007163b', 18, 'LugaETH', 'LRT', 'Luganodes', 'symbiotic')
        ,('0xd6e09a5e6d719d1c881579c9c8670a210437931b', 18, 'coETH', 'LRT', 'ChorusOne', 'symbiotic')
        ,('0x84631c0d0081fde56deb72f6de77abbbf6a9f93a', 18, 'Re7LRT', 'LRT', 'Re7Labs', 'symbiotic')
        
        -- karak lrts 
        ,('0x7223442cad8e9cA474fC40109ab981608F8c4273', 18, 'weETHk', 'LRT', 'ether.fi', 'karak')
        
) as temp_table (token_address, token_decimals, token_symbol, token_category, token_project, lrt_project)
