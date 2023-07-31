{{ config(
        alias = alias('erc20_all')
        ,tags=['dunesql']
        ,materialized='incremental'
        ,file_format = 'delta'
        ,unique_key = ['blockchain','contract_address']
        ,post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","optimism", "gnosis", "fantom", "polygon"]\',
                                    "sector",
                                    "tokens",
                                    \'["msilb7"]\') }}')}}


-- This is a table to store all contracts that are ERC20s. 
-- This should be combined in to the main ERC20 table, such that it mirrors how the NFT table works.
-- But to avoid screwing anything up, keeping this as separate in the interim.
-- This table is REQUIRED for contracts mapping spells.

-- DO NOT DELETE

{% set evm_chains = all_evm_mainnets_testnets_chains() %} --macro: all_evm_mainnets_testnets_chains.sql

        SELECT blockchain, contract_address, 'erc20' as standard

        from {{ source('erc20_' + chain , 'evt_transfer') }} tr 

        WHERE 1=1
        {% if is_incremental() %}
        and tr.evt_block_time >= date_trunc('day', now() - interval '7' day)
        AND contract_address NOT IN (
                                SELECT contract_address
                                from {{this}} t
                                WHERE t.blockchain = '{{chain}}'
                                )
        {% endif %}
        GROUP BY 1,2,3 --uniques

{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}