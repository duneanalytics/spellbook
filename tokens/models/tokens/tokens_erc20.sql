{{ config( alias = 'erc20',
        tags=['static'],
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","optimism", "gnosis", "fantom", "polygon","base", "celo", "zksync"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xManny","hildobby","soispoke","dot2dotseurat","mtitus6","wuligy","lgingerich"]\') }}')}}


SELECT 'ethereum' as blockchain, contract_address, symbol, decimals FROM  {{ ref('tokens_ethereum_erc20') }}
