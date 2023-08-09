{{ config( alias = alias('erc20', legacy_model=True), tags=['static', 'legacy'])}}

SELECT LOWER(contract_address) as contract_address, symbol, decimals
FROM (VALUES
        ('0x4200000000000000000000000000000000000006', 'WETH', 18)
        ,('0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22', 'cbETH', 18)
        ,('0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA', 'USDbC', 6)
        ,('0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb', 'DAI', 18)
     ) AS temp_table (contract_address, symbol, decimals)
