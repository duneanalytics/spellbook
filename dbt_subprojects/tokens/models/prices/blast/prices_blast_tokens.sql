{{ config(
        schema='prices_blast',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags=['static']
        )
}}
SELECT
    token_id
    , blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES
    ('weth-weth','blast','WETH',0x4300000000000000000000000000000000000004,18),
    ('usdb-usdb','blast','USDB',0x4300000000000000000000000000000000000003,18),
    ('weth-weth', 'blast', 'bpETH', 0xb772d5c5f4a2eef67dfbc89aa658d2711341b8e5, 18),
    ('yolo-yolo-games','blast','YOLO',0xf77dd21c5ce38ac08786be35ef1d1dec1a6a15f3,18),
    ('blast-blast-token','blast','BLAST',0xb1a5700fa2358173fe465e6ea4ff52e36e88e2ad,18),
    ('bag-bagwin','blast','BAG',0xb9dfcd4cf589bb8090569cb52fac1b88dbe4981f,18),
    ('usdz-anzen-usdz','blast','USDz',0x52056ed29fe015f4ba2e3b079d10c0b87f46e8c6,18),
    ('bpepe-blastin-pepes','blast','bPEPE',0xb6e0d8a730c6e5c85c637b1cf7ad6fd07927b965,18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
