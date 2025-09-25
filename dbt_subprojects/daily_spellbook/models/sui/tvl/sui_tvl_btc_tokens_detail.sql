{{ config(
    schema='sui_tvl',
    alias='btc_tokens_detail',
    materialized='table',
    tags=['sui','tvl','btc','tokens','whitelist']
) }}

-- BTC token metadata reference table
-- Serves as a whitelist filter for all BTC token variants in TVL calculations
-- Matches Snowflake BTC_TOKENS_DETAIL pattern for Sui ecosystem

select * from (
    values
        -- Standard wrapped BTC (most common)
        ('0x2::coin::TreasuryCap<0x27792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN>', 
         lower('0x27792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN'), 
         8, 'Wrapped Bitcoin', 'BTC'),
        
        -- tBTC (Threshold Network)
        ('0x77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1::Gateway::GatewayCapabilities', 
         lower('0x77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1::TBTC::TBTC'), 
         8, 'tBTC', 'TBTC'),
         
        -- SatLayer BTC (Babylon staking)
        ('0x25646e1cac13d6198e821aac7a94cbb74a8e49a2b3bed2ffd22346990811fcc6::satlayer_pool::Vault<>', 
         lower('0xdfe175720cb087f967694c1ce7e881ed835be73e8821e161c351f4cea24a0f20::satlbtc::SATLBTC'), 
         8, 'SatLayer BTC', 'SATLBTC')
         
        -- Add more BTC variants as they emerge in the Sui ecosystem
        -- Examples could include: cbBTC, hBTC, renBTC, etc.
         
) as t(type_, coin_type, coin_decimals, coin_name, coin_symbol) 