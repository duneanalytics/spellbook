{{ config(
	tags=['legacy'],
	
    alias = alias('addresses_gnosis_colony', legacy_model=True),
    partition_by = ['created_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_block_time', 'dao_wallet_address', 'blockchain', 'dao', 'dao_creator_tool']
    )
}}

{% set project_start_date = '2020-09-08' %}

WITH -- dune query here - https://dune.com/queries/1435493

get_colony_wallets as ( -- getting colonies created through colony 
        SELECT 
            block_time as created_block_time, 
            date_trunc('day', block_time) as created_date, 
            CONCAT('0x', RIGHT(topic3, 40)) as colony
        FROM 
        {{ source('gnosis', 'logs') }}
        {% if not is_incremental() %}
        WHERE block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND topic1 = '0x1904953a6126b2f999ad2661494642bfc63346430965de35cdcd7b5d4e6787ae' -- colony added event that is emitted when a colony is created 
        AND contract_address = '0x78163f593d1fa151b4b7cacd146586ad2b686294' -- colony factory contract address 
)

SELECT 
    'gnosis' as blockchain, 
    'colony' as dao_creator_tool, 
    colony as dao, 
    colony as dao_wallet_address, -- the colony address is also the address that receives & sends funds 
    created_block_time, 
    TRY_CAST(created_date as DATE) as created_date
FROM 
get_colony_wallets