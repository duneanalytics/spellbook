{{ 
    config(
        tags = ['dunesql'],
        schema = 'nft_celo',
        alias = alias('aggregators'),
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "nft",
                                    \'["tomfutago"]\') }}'
    )
}}

select contract_address, name
from (
    values
    (0x5Dc88340E1c5c6366864Ee415d6034cadd1A9897, 'Uniswap') -- Uniswap's Universal Router 3
) as temp_table (contract_address, name)
