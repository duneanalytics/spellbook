{{ config(
    schema='sui_tvl',
    alias='key_tokens_detail',
    materialized='table',
    tags=['sui','tvl','tokens','whitelist']
) }}

-- Key token metadata reference table
-- Serves as a whitelist filter for important tokens in TVL calculations
-- Adapted from Snowflake pattern for Sui ecosystem

select * from (
    values
        -- Native SUI (using lowercase like existing patterns)
        ('0x2::coin::CoinMetadata<0x2::sui::SUI>', 
         lower('0x2::sui::SUI'), 
         9, 'Sui', 'SUI'),
        
        -- Wrapped Bitcoin tokens (common in Sui ecosystem)  
        ('0x2::coin::CoinMetadata<0x27792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN>', 
         lower('0x27792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN'), 
         8, 'Wrapped Bitcoin', 'BTC'),
         
                 -- USDC (common stablecoin)
         ('0x2::coin::CoinMetadata<0x5d4b302506645c37ff133b98c4b50a5ae14841659738d6d733d59d0d217a93bf::coin::COIN>', 
          lower('0x5d4b302506645c37ff133b98c4b50a5ae14841659738d6d733d59d0d217a93bf::coin::COIN'), 
          6, 'USD Coin', 'USDC'),
          
         -- USDT (common stablecoin)  
         ('0x2::coin::CoinMetadata<0xc060006111016b8a020ad5b33834984a437aaa7d3c74c18e09a95d48aceab08c::coin::COIN>', 
          lower('0xc060006111016b8a020ad5b33834984a437aaa7d3c74c18e09a95d48aceab08c::coin::COIN'), 
          6, 'Tether USD', 'USDT'),
          
         -- BUCK (Bucket protocol stablecoin)
         ('0x2::coin::CoinMetadata<0x9e3dab13212b27f5434416939db5dec6f6717822e825121b82320b9e8503bade::buck::BUCK>', 
          lower('0x9e3dab13212b27f5434416939db5dec6f6717822e825121b82320b9e8503bade::buck::BUCK'), 
          9, 'BUCK Stablecoin', 'BUCK')
         
) as t(type_, coin_type, coin_decimals, coin_name, coin_symbol) 