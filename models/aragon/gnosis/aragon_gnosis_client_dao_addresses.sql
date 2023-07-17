{{ config(
    alias = alias('client_dao_addresses'),
    partition_by = ['created_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_block_time', 'dao_wallet_address', 'blockchain', 'dao', 'dao_creator_tool']
    )
}}

{% set project_start_date = '2020-05-24' %}

WITH  -- dune query here - https://dune.com/queries/1435985

-- this code follows the same logic as dao_addresses_ethereum_aragon, Refer to that for comments on code.

aragon_daos as (
        SELECT
            block_time as created_block_time,
            date_trunc('day', block_time) as created_date,
            CONCAT('0x', SUBSTRING(data, 27, 40)) as dao
        FROM
        {{ source('gnosis', 'logs') }}
        {% if not is_incremental() %}
        WHERE block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND topic1 = '0x3a7eb042a769adf51e9be78b68ed7af0ad7b379246536efc376ed2ca01238282' -- deploy dao event on aragon
),

app_ids (app_id) as (
        VALUES
            (LOWER('0x9ac98dc5f995bf0211ed589ef022719d1487e5cb2bab505676f0d084c07cf89a')), -- agent
            (LOWER('0x701a4fd1f5174d12a0f1d9ad2c88d0ad11ab6aad0ac72b7d9ce621815f8016a9')), -- agent
            (LOWER('0xf2e5eb0f21694bf4e28f98a980dfc4d6a568b5b3e593cfe9cedfd0aed59d8148')), -- agent
            (LOWER('0xbf8491150dafc5dcaee5b861414dca922de09ccffa344964ae167212e8c673ae')), -- finance
            (LOWER('0x5c9918c99c4081ca9459c178381be71d9da40e49e151687da55099c49a4237f1')), -- finance
            (LOWER('0xa9efdd08ab8a16b35803b9887d721f0b9cf17df8ff66b9e57f23bbe4ae5f18ba')), -- finance
            (LOWER('0x7e852e0fcfce6551c13800f1e7476f982525c2b5277ba14b24339c68416336d1')) -- vault
),

get_aragon_wallets as (
        SELECT
            contract_address as dao,
            CONCAT('0x', SUBSTRING(data, 27, 40)) as dao_wallet_address
        FROM
        {{ source('gnosis', 'logs') }}
        {% if not is_incremental() %}
        WHERE block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND topic1 = LOWER('0xd880e726dced8808d727f02dd0e6fdd3a945b24bfee77e13367bcbe61ddbaf47')
        AND CONCAT('0x', SUBSTRING(data, 131, 64)) IN (SELECT app_id FROM app_ids)
)

SELECT 
    'gnosis' as blockchain, 
    'aragon' as dao_creator_tool, 
    ad.dao, 
    gw.dao_wallet_address, 
    ad.created_block_time,
    TRY_CAST(ad.created_date as DATE) as created_date, 
    'aragon_client' as product
FROM 
aragon_daos ad 
LEFT JOIN 
get_aragon_wallets gw  -- left join to get the dao address mapped to the app address
    ON ad.dao = gw.dao 