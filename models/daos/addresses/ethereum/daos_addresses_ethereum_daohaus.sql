{{ config(
    alias = 'daos_addresses_ethereum_daohaus',
    partition_by = ['created_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_date', 'blockchain', 'dao', 'dao_creator_tool']
    )
}}

{% set moloch_start_date = '2021-01-25' %}

{% set minion_start_date = '2021-09-17' %}

WITH  -- dune query here - https://dune.com/queries/1433790

get_daohaus_molochs as (
        SELECT 
            block_time as created_block_time, 
            date_trunc('day', block_time) as created_date, 
            CONCAT('0x', RIGHT(topic2, 40)) as moloch
        FROM 
        {{ source('ethereum', 'logs') }}
        {% if not is_incremental() %}
        WHERE block_time >= '{{moloch_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND topic1 = '0x099e0b09e056ad33e22e4d35de2e837a30ba249f33d912abb7e1e273bbf9d650'
        AND contract_address = '0x38064f40b20347d58b326e767791a6f79cdeddce'
), 

get_minion_creations as (
        SELECT 
            CONCAT('0x', RIGHT(topic3, 40)) as moloch,  
            CONCAT('0x', RIGHT(topic2, 40)) as wallet_address
        FROM 
        {{ source('ethereum', 'logs') }}
        {% if not is_incremental() %}
        WHERE block_time >= '{{minion_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND topic1 = '0xbaefe449c0963ab3bd87eb56115a3f8420fbefae45878f063cc59a6cb99d3ae0'
        AND contract_address IN ('0x594af060c08eea9f559bc668484e50596bcb2cfb', '0xbc37509a283e2bb67fd151c34e72e826c501e108')
), 

get_daohaus_wallets as (
        SELECT 
            gm.created_date, 
            gm.created_block_time, 
            gm.moloch as dao, 
            gm.moloch as dao_wallet, 
            gc.wallet_address as minion_wallet
        FROM 
        get_daohaus_molochs gm 
        LEFT JOIN 
        get_minion_creations gc 
            ON gm.moloch = gc.moloch
), 

mapped_wallets as (
        SELECT 
            'ethereum' as blockchain, 
            'dao-haus' as dao_creator_tool, 
            dao, 
            dao_wallet as dao_wallet_address, 
            created_date, 
            created_block_time 
        FROM 
        get_daohaus_wallets

        UNION 

        SELECT 
            'ethereum' as blockchain, 
            'dao-haus' as dao_creator_tool, 
            dao, 
            minion_wallet as dao_wallet_address,
            created_date, 
            created_block_time 
        FROM 
        get_daohaus_wallets
)

SELECT 
    DISTINCT(mw.*)
FROM 
mapped_wallets mw 
WHERE dao_wallet_address IS NOT NULL 