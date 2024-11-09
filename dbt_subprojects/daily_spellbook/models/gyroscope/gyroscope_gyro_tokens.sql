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
    (0xD9FBA68D89178e3538e708939332c79efC540179, 'stataArbGHO', 18, 'arbitrum'),    
    (0x89AEc2023f89E26Dbb7eaa7a98fe3996f9d112A8, 'stataArbFRAX', 18, 'arbitrum'),      
    (0xC509aB7bB4eDbF193b82264D499a7Fc526Cd01F4, 'stataAvaUSDC', 6, 'avalanche_c'),    
    (0x4EA71A20e655794051D1eE8b6e4A3269B13ccaCc, 'stataBasUSDC', 6, 'base'),
    (0x270ba1f35d8b87510d24f693fccc0da02e6e4eeb, 'stataGnoUSDC', 6, 'base'),
    (0x862c57d48becB45583AEbA3f489696D22466Ca1b, 'stataEthUSDT', 6, 'ethereum'),
    (0x848107491e029afde0ac543779c7790382f15929, 'stataEthcrvUSD', 18, 'ethereum'),    
    (0xDBf5E36569798D1E39eE9d7B1c61A7409a74F23A, 'stataEthLUSD', 18, 'ethereum'),        
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
