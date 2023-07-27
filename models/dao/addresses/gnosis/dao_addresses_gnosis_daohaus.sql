{{ config(
	tags=['legacy'],
	
    alias = alias('addresses_gnosis_daohaus', legacy_model=True),
    partition_by = ['created_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_block_time', 'dao_wallet_address', 'blockchain', 'dao', 'dao_creator_tool']
    )
}}

{% set moloch_start_date = '2021-01-06' %}

{% set minion_start_date = '2021-09-13' %}

-- this code follows the same logic as dao_addresses_ethereum_daohaus, Refer to that for comments on code.

WITH -- dune query here - https://dune.com/queries/1434676

get_daohaus_molochs as (
        SELECT 
            block_time as created_block_time, 
            TRY_CAST(date_trunc('day', block_time) as DATE) as created_date, 
            CONCAT('0x', RIGHT(topic2, 40)) as moloch
        FROM 
        {{ source('gnosis', 'logs') }}
        {% if not is_incremental() %}
        WHERE block_time >= '{{moloch_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND topic1 = '0x099e0b09e056ad33e22e4d35de2e837a30ba249f33d912abb7e1e273bbf9d650'
        AND contract_address = '0x0f50b2f3165db96614fbb6e4262716acc9f9e098'
), 

get_minion_creations as (
        SELECT 
            CONCAT('0x', RIGHT(topic3, 40)) as moloch,  
            CONCAT('0x', RIGHT(topic2, 40)) as wallet_address
        FROM 
        {{ source('gnosis', 'logs') }}
        {% if not is_incremental() %}
        WHERE block_time >= '{{minion_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND topic1 = '0xbaefe449c0963ab3bd87eb56115a3f8420fbefae45878f063cc59a6cb99d3ae0'
        AND contract_address IN (LOWER('0xA1b97D22e22507498B350A9edeA85c44bA7DBC01'), LOWER('0xBD090EF169c0C8589Acb33406C29C20d22bb4a55'))
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
            'gnosis' as blockchain, 
            'dao-haus' as dao_creator_tool, 
            dao, 
            dao_wallet as dao_wallet_address, 
            created_block_time, 
            created_date
        FROM 
        get_daohaus_wallets

        UNION 

        SELECT 
            'gnosis' as blockchain, 
            'dao-haus' as dao_creator_tool, 
            dao, 
            minion_wallet as dao_wallet_address,
            created_block_time, 
            created_date 
        FROM 
        get_daohaus_wallets
)

SELECT 
    DISTINCT(mw.*)
FROM 
mapped_wallets mw 
WHERE dao_wallet_address IS NOT NULL 