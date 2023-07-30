{{ config( alias = alias('erc20_all'),
        tags=['dunesql'],
        ,materialized='incremental'
        ,file_format = 'delta'
        ,unique_key = ['blockchain','contract_address']
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","optimism", "gnosis", "fantom", "polygon"]\',
                                    "sector",
                                    "tokens",
                                    \'["msilb7"]\') }}')}}


-- This is a table to store all contracts that are ERC20s. 
-- This should be combined in to the main ERC20 table, such that it mirrors how the NFT table works.
-- But to avoid screwing anything up, keeping this as separate in the interim.
-- This table is REQUIRED for contracts mapping spells.

-- DO NOT DELETE

SELECT

blockchain, token_address AS contract_address, 'erc20' as standard

FROM {{ ref('transfers_erc20')}} tr 

WHERE 1=1
{% if is_incremental() %}
and tr.evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

GROUP BY 1,2