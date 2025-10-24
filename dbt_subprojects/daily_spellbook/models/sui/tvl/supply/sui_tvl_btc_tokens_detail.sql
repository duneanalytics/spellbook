{{ config(
    schema='sui_tvl',
    alias='btc_tokens_detail',
    materialized='table',
    tags=['sui','tvl','supply','btc','tokens']
) }}

-- BTC Tokens Detail: Comprehensive configuration for all BTC ecosystem tokens
-- This serves as the authoritative source for BTC token metadata on Sui
-- Based on the provided Snowflake BTC_TOKENS_DETAIL structure

select * from (
    values
        ('0x2::coin::CoinMetadata<0x027792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN>', '0x027792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN', 8, 'Wrapped BTC', 'WBTC'),
        ('0x2::coin::CoinMetadata<0xaafb102dd0902f5055cadecd687fb5b71ca82ef0e0285d90afde828ec58ca96b::btc::BTC>', '0xaafb102dd0902f5055cadecd687fb5b71ca82ef0e0285d90afde828ec58ca96b::btc::BTC', 8, 'Wrapped Bitcoin', 'wBTC'),
        ('0x2::coin::CoinMetadata<0xdfe175720cb087f967694c1ce7e881ed835be73e8821e161c351f4cea24a0f20::satlbtc::SATLBTC>', '0xdfe175720cb087f967694c1ce7e881ed835be73e8821e161c351f4cea24a0f20::satlbtc::SATLBTC', 8, 'satLBTC', 'SATLBTC'),
        ('0x2::coin::CoinMetadata<0x8bb49770175de6bd20ba98b58f3e34efe2a9424b3b80d2cb6dff2edd1e23dbc7::fbtc::FBTC>', '0x8bb49770175de6bd20ba98b58f3e34efe2a9424b3b80d2cb6dff2edd1e23dbc7::fbtc::FBTC', 8, 'FBTC', 'FBTC'),
        ('0x2::coin::CoinMetadata<0x8f2b5eb696ed88b71fea398d330bccfa52f6e2a5a8e1ac6180fcb25c6de42ebc::coin::COIN>', '0x8f2b5eb696ed88b71fea398d330bccfa52f6e2a5a8e1ac6180fcb25c6de42ebc::coin::COIN', 8, 'Lorenzo Wrapped Bitcoin', 'enzoBTC'),
        ('0x2::coin::CoinMetadata<0xd1a91b46bd6d966b62686263609074ad16cfdffc63c31a4775870a2d54d20c6b::mbtc::MBTC>', '0xd1a91b46bd6d966b62686263609074ad16cfdffc63c31a4775870a2d54d20c6b::mbtc::MBTC', 8, 'MBTC', 'MBTC'),
        ('0x2::coin::CoinMetadata<0xa03ab7eee2c8e97111977b77374eaf6324ba617e7027382228350db08469189e::ybtc::YBTC>', '0xa03ab7eee2c8e97111977b77374eaf6324ba617e7027382228350db08469189e::ybtc::YBTC', 8, 'Yield BTC.B', 'YBTC.B'),
        ('0x2::coin::CoinMetadata<0x5f496ed5d9d045c5b788dc1bb85f54100f2ede11e46f6a232c29daada4c5bdb6::coin::COIN>', '0x5f496ed5d9d045c5b788dc1bb85f54100f2ede11e46f6a232c29daada4c5bdb6::coin::COIN', 8, 'Lorenzo stBTC', 'stBTC'),
        ('0x2::coin::CoinMetadata<0x876a4b7bce8aeaef60464c11f4026903e9afacab79b9b142686158aa86560b50::xbtc::XBTC>', '0x876a4b7bce8aeaef60464c11f4026903e9afacab79b9b142686158aa86560b50::xbtc::XBTC', 8, 'OKX Wrapped BTC', 'xBTC'),
        ('0x2::coin::CoinMetadata<0x3e8e9423d80e1774a7ca128fccd8bf5f1f7753be658c5e645929037f7c819040::lbtc::LBTC>', '0x3e8e9423d80e1774a7ca128fccd8bf5f1f7753be658c5e645929037f7c819040::lbtc::LBTC', 8, 'Lombard Staked BTC', 'LBTC'),
        ('0x2::coin::CoinMetadata<0x77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1::TBTC::TBTC>', '0x77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1::TBTC::TBTC', 8, 'tBTC v2', 'TBTC')
) as t(type_, coin_type, coin_decimals, coin_name, coin_symbol) 