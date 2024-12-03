{{
    config(
        schema = 'balancer_v3_gnosis',
        alias = 'static_tokens_mapping', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH aave_tokens AS(

SELECT 
    static_atoken,
    static_atoken_name,
    static_atoken_symbol,
    underlying_token,
    underlying_token_symbol,
    underlying_token_decimals
FROM (VALUES 
     (0x2d737e2b0e175f05d0904c208d6c4e40da570f65, 'Static Aave Gnosis GNO', 'stataGnoGNO', 0x9C58BAcC331c9aa871AFD802DB6379a98e80CEdb, 'GNO', 18),
     (0xecfd0638175e291ba3f784a58fb9d38a25418904, 'Static Aave Gnosis wstETH', 'stataGnowstETH', 0x6c76971f98945ae98dd7d4dfca8711ebea946ea6, 'wstETH', 18),
     (0xd843fb478c5aa9759fea3f3c98d467e2f136190a, 'Static Aave Gnosis WETH', 'stataGnoWETH', 0x6a023ccd1ff6f2045c3309768ead9e68f978f6e1, 'WETH', 18),
     (0x270ba1f35d8b87510d24f693fccc0da02e6e4eeb, 'Static Aave Gnosis USDC', 'stataGnoUSDC', 0x2a22f9c3b484c3629090FeED35F17Ff8F88f76F0, 'USDC', 6)
    ) AS temp_table (static_atoken, static_atoken_name, static_atoken_symbol, underlying_token, underlying_token_symbol, underlying_token_decimals)
)

SELECT 
    'gnosis' AS blockchain, 
    * 
FROM aave_tokens