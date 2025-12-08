{{
    config(
        schema = 'tokens_xlayer_v1'
        ,alias = 'erc20'
        ,tags = ['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address
    , symbol
    , decimals
FROM 
(
    VALUES
    -- placeholder rows to give example of format
    (0x5831f949D6A239Cd1CDBaC652A060f0837b0CAc0, 'OKB', 18)
)
AS temp_table (contract_address, symbol, decimals)
