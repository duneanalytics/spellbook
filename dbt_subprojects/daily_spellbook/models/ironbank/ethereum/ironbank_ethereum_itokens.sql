{{ config(
    
    alias = 'itokens',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "ironbank",
                                \'["michael-ironbank"]\') }}'
) }}

SELECT
    symbol, 
    contract_address, 
    decimals, 
    underlying_token_address, 
    underlying_decimals, 
    underlying_symbol
FROM
    (
        VALUES
            ('iCVX', 0xe0b57feed45e7d908f2d0dacd26f113cf26715bf, 8, 0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b, 18, 'CVX'),
            ('iMIM', 0x9e8e207083ffd5bdc3d99a1f32d1e6250869c1a9, 8, 0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3, 18, 'MIM'),
            ('iZAR', 0x672473908587b10e65dab177dbaeadcbb30bf40b, 8, 0x81d66d255d47662b6b16f3c5bbfbb15283b05bc2, 18, 'ZAR'),
            ('iCRV', 0xb8c5af54bbdcc61453144cf472a9276ae36109f9, 8, 0xd533a949740bb3306d119cc777fa900ba034cd52, 18, 'CRV'),
            ('iAAVE', 0x30190a3b52b5ab1daf70d46d72536f5171f22340, 8, 0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9, 18, 'AAVE'),
            ('iCHF', 0x1b3e95e8ecf7a7cab6c4de1b344f94865abd12d5, 8, 0x1cc481ce2bd2ec7bf67d1be64d4878b16078f309, 18, 'ibCHF'),
            ('iGBP', 0xecab2c76f1a8359a06fab5fa0ceea51280a97ecf, 8, 0x69681f8fde45345c3870bcd5eaf4a05a60e7d227, 18, 'ibGBP'),
            ('iAUD', 0x86bbd9ac8b9b44c95ffc6baae58e25033b7548aa, 8, 0xfafdf0c4c1cb09d430bf88c75d88bb46dae09967, 18, 'ibAUD'),
            ('iJPY', 0x215f34af6557a6598dbda9aa11cc556f5ae264b1, 8, 0x5555f75e3d5278082200fb451d1b6ba946d8e13b, 18, 'ibJPY'),
            ('iKRW', 0x3c9f5385c288ce438ed55620938a4b967c080101, 8, 0x95dfdc8161832e4ff7816ac4b6367ce201538253, 18, 'ibKRW'),
            ('iEUR', 0x00e5c0774a5f065c285068170b20393925c84bf3, 8, 0x96e61422b6a9ba0e068b6c5add4ffabc6a4aae27, 18, 'ibEUR'),
            ('iUSDC', 0x76eb2fe28b36b3ee97f3adae0c69606eedb2a37c, 8, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 6, 'USDC'),
            ('iLINK', 0xe7bff2da8a2f619c2586fb83938fa56ce803aa16, 8, 0x514910771af9ca656af840dff83e8264ecf986ca, 18, 'LINK'),
            ('iYFI', 0xfa3472f7319477c9bfecdd66e4b948569e7621b9, 8, 0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e, 18, 'YFI'),
            ('iSNX', 0x12a9cc33a980daa74e00cc2d1a0e74c57a93d12c, 8, 0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f, 18, 'SNX'),
            ('iWBTC', 0x8fc8bfd80d6a9f17fb98a373023d72531792b431, 8, 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599, 8, 'WBTC'),
            ('iEURS', 0xa8caea564811af0e92b1e044f3edd18fa9a73e4f, 8, 0xdb25f211ab05b1c97d595516f45794528a807ad8, 2, 'EURS'),
            ('iSEUR', 0xca55f9c4e77f7b8524178583b0f7c798de17fd54, 8, 0xd71ecff9342a5ced620049e616c5035f1db98620, 18, 'SEUR'),
            ('iDPI', 0x7736ffb07104c0c400bb0cc9a7c228452a732992, 8, 0x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b, 18, 'DPI'),
            ('iSUSD', 0xa7c4054afd3dbbbf5bfe80f41862b89ea05c9806, 8, 0x57ab1ec28d129707052df4df418d58a2d46d5f51, 18, 'SUSD'),
            ('iUSDT', 0x48759f220ed983db51fa7a8c0d2aab8f3ce4166a, 8, 0xdac17f958d2ee523a2206206994597c13d831ec7, 6, 'USDT'),
            ('iDAI', 0x8e595470ed749b85c6f7669de83eae304c2ec68f, 8, 0x6b175474e89094c44da98b954eedeac495271d0f, 18, 'DAI'),
            ('iWETH', 0x41c84c0e2ee0b740cf0d31f63f3b6f627dc6b393, 8, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18, 'WETH'),
            ('iSUSHI', 0x226f3738238932ba0db2319a8117d9555446102f, 8, 0x6b3595068778dd592e39a122f4f5a5cf09c90fe2, 18, 'SUSHI'),
            ('iUNI', 0xfeeb92386a055e2ef7c2b598c872a4047a7db59f, 8, 0x1f9840a85d5af5bf1d1762f925bdaddc4201f984, 18, 'UNI'),
            ('iWSTETH', 0xbc6b6c837560d1fe317ebb54e105c89f303d5afd, 8, 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0, 18, 'wstETH')
    ) AS temp_table (
        symbol, 
        contract_address, 
        decimals, 
        underlying_token_address, 
        underlying_decimals, 
        underlying_symbol
    )
