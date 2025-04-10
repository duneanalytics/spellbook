{{
    config(
        schema = 'tokens_worldchain'
        ,alias = 'erc20'
        ,tags = ['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address
    , symbol
    , decimals
FROM (VALUES
    (0x2cFc85d8E48F8EAB294be644d9E25C3030863003, 'WLD', 18)
    , (0x79A02482A880bCE3F13e09Da970dC34db4CD24d1, 'USDC.e', 6)
    , (0x03C7054BCB39f7b2e5B2c7AcB37583e32D70Cfa3, 'WBTC', 8)
    , (0x4200000000000000000000000000000000000006, 'WETH', 18)
    , (0x859DBE24b90C9f2f7742083d3cf59cA41f55Be5d, 'sDAI', 18)
    , (0xcd1E32B86953D79a6AC58e813D2EA7a1790cAb63, 'ORO', 18)
    , (0x102d758f688a4c1c5a80b116bd945d4455460282, 'USD₮0', 6)
    , (0xede54d9c024ee80c85ec0a75ed2d8774c7fbac9b, 'WDD', 18)
    , (0x361990637f68f2e52b6e38194330bb3964b0e908, 'EGG', 18)
    , (0xf3f92a60e6004f3982f0fde0d43602fc0a30a0db, 'ORB', 18)
    , (0x1ae3498f1b417fe31be544b04b711f27ba437bd3, 'PUF', 18)
    , (0x80e46ba4486f005f51ed9e31e2e479286edc6877, 'MEOW', 18)
    , (0xad3ee0342cb753c2b39579f9db292a9ae94b153e, 'GEMS', 18)
    , (0x998e211ecf67e09c53b2d95805d735135ac50a5a, 'EGG', 18)
    , (0x00471c596755c5eb197a3e43683534b4858c2c0f, 'BOTR', 18)
    , (0x1f4902fe0694c225d5ea639036bca87b380d7d83, 'TLEG', 18)
    , (0x0790ec1865be3e92df570e6d7d2b79bce9c6e2fb, 'PULSE', 18)
    , (0x893e03689f9e668304b5702552a47a305451bda1, 'WLDM', 18)
    , (0x394d1123dbbbfbdbe1bb0450117cdf06a30e394d, 'WLD Pi', 18)
    , (0x90ac15b9fadc4656dce25c8da0d2bce6a16e605c, 'WTD', 18)
    , (0x73ac70d48832ba8da9d7ddaae5fe03f8f1ed2928, 'ROLU', 18)
    , (0xa9c5d1f2036850e035afad88ef75c550df9dc22e, 'Mask', 18)
    , (0x9b8df6e244526ab5f6e6400d331db28c8fdddb55, 'uSOL', 18)
    , (0x41f42a577d557c4dead70230bc654a52584053a5, 'EGG', 18)
    , (0x38d121e42d1bcfdfc3cc7cb1ceed7a13416edcad, 'WLP', 18)
    , (0x2615a94df961278dcbc41fb0a54fec5f10a693ae, 'uXRP', 18)
) AS temp_table (contract_address, symbol, decimals)
