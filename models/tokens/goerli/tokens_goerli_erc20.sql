{{ config(
    alias = 'erc20'
    , schema = 'tokens_goerli'
    , tags=['static']
)}}

SELECT contract_address as contract_address, symbol, decimals
FROM (VALUES
       (0xa3e0Dfbf8DbD86e039f7CDB37682A776D66dae4b, 'USDC', 6)
       ) AS temp_table (contract_address, symbol, decimals)
