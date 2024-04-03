{{ config(
    schema = 'gyroscope',
    alias = 'gyro_tokens'
    )
}}

    --These tokens are priced at 1 USD, a value that tends to grow marginally over time. Since they have just been launched, we won't be able to price them via dex_prices at first. As this changes, this table can be discarded.
WITH gyro_tokens as (
    SELECT * FROM (values
    (0x7CFaDFD5645B50bE87d546f42699d863648251ad, 'stataArbUSDCn', 6, 'arbitrum'),
    (0xb165a74407fE1e519d6bCbDeC1Ed3202B35a4140, 'stataArbUSDT', 6, 'arbitrum'),
    (0x862c57d48becB45583AEbA3f489696D22466Ca1b, 'stataEthUSDT', 6, 'ethereum'),
    (0x87A1fdc4C726c459f597282be639a045062c0E46, 'stataPolUSDT', 6, 'polygon'),
    (0x2dCa80061632f3F87c9cA28364d1d0c30cD79a19, 'stataPolUSDCn', 6, 'polygon'),        
    (0x4dd03dfd36548c840b563745e3fbec320f37ba7e, 'stataOptUSDCn', 6, 'optimism'),
    (0x035c93db04E5aAea54E6cd0261C492a3e0638b37, 'stataOptUSDT', 6, 'optimism'))
        as t (address, name, decimals, blockchain))
    
SELECT 
    blockchain,
    address AS token_address,
    name AS token_symbol,
    decimals,
    1 AS price
FROM gyro_tokens
