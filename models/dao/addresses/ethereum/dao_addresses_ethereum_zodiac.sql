{{ config(
    alias = alias('addresses_ethereum_zodiac'),
    partition_by = ['created_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_block_time', 'dao_wallet_address', 'blockchain', 'dao', 'dao_creator_tool']
    )
}}

{% set project_start_date = '2021-09-15' %}

WITH  -- dune query here https://dune.com/queries/1433654

get_zodiac_wallets as ( -- getting the gnosis safes created using zodiac's reality.eth module
        SELECT 
            block_time as created_block_time, 
            TRY_CAST(date_trunc('day', block_time) as DATE) as created_date, 
            CONCAT('0x', RIGHT(topic3, 40)) as dao
        FROM 
        {{ source('ethereum', 'logs') }}
        {% if not is_incremental() %}
        WHERE block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND topic1 = '0x8b8abdce7435e63696dbae9e46dc2ee5036195638ecfc5b45a3c45bcd7e3ed34' -- module set up event emitted when a reality.eth module is set up 
)

SELECT 
    'ethereum' as blockchain, 
    'zodiac' as dao_creator_tool, 
    dao, 
    dao as dao_wallet_address, 
    MIN(created_block_time) as created_block_time, 
    MIN(created_date) as created_date -- using this to get the created date as the first time the module was set up, it's possible to disable and renable a module. 
FROM 
get_zodiac_wallets
GROUP BY 1, 2, 3, 4 
