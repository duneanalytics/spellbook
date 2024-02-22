{{ config(
    tags=[ 'static']
    , schema = 'tokens_zora'
    , alias = 'erc20'
    , materialized = 'table'
    , post_hook='{{ expose_spells(\'["zora"]\',
                                    "sector",
                                    "tokens",
                                    \'["hildobby","msilb7"]\') }}'
    )
}}

SELECT contract_address, symbol, decimals
FROM (VALUES
        (0x4200000000000000000000000000000000000006, 'WETH', 18)
     ) AS temp_table (contract_address, symbol, decimals)
