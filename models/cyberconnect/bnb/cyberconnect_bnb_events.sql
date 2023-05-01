{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'name', 'evt_tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "cyberconnect",
                                \'["NazihKalo"]\') }}')
}}


{% set project_start_date = '2022-09-11' %}

with 
-------CYBERCONNECT
cyberconnect_profile_create as 
(
  select 
            'bnb' as blockchain,
            'cyberconnect' project,
            'create' action,
            'CreateProfile' name,
            a.contract_address,
            a.evt_block_time,
            date_trunc('day', a.evt_block_time) AS block_date,
            a.evt_block_number,
            'Mint' evt_type,
            a.evt_tx_hash,
            a.evt_index,
            a.to buyer,
            '0x0000000000000000000000000000000000000000' as seller,
            a.handle,
            a.profileId profile_id,
            cast(NULL as uint256) AS  content_id,            
            metadata content_uri
    from {{source('link3_profile_bnb', 'ProfileNFT_evt_CreateProfile')}} a
    WHERE 1=1
     {% if is_incremental() %}
    AND a.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND a.evt_block_time >= '{{project_start_date}}'
    {% endif %}

),
    
cyberconnect_essence_register as (
    select 
            'bnb' as blockchain,
            'cyberconnect' project,
            'create' action,
            'RegisterEssence' name,
            a.contract_address,
            a.evt_block_time,
            date_trunc('day', a.evt_block_time) AS block_date,
            a.evt_block_number,
            'Mint' evt_type,
            a.evt_tx_hash,
            a.evt_index,
            b.to buyer,
            '0x0000000000000000000000000000000000000000' as seller,
            b.handle,
            a.profileId profile_id,
            a.essenceId content_id,
            a.essenceTokenURI content_uri
    from {{source('link3_profile_bnb', 'ProfileNFT_evt_RegisterEssence')}} a
    left join {{source('link3_profile_bnb', 'ProfileNFT_evt_CreateProfile')}} b on a.profileId = b.profileId
    WHERE 1=1
    {% if is_incremental() %}
    AND a.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND a.evt_block_time >= '{{project_start_date}}'
    {% endif %}
),

-- essence collect by profile or address
cyberconnect_essence_collect as (
    select 
            'bnb' as blockchain,
            'cyberconnect' project,
            'collect' action,
            'CollectEssence' name,
            a.contract_address,
            a.evt_block_time,
            date_trunc('day', a.evt_block_time) AS block_date,
            a.evt_block_number,
            a.evt_tx_hash,
            a.evt_index,
            'Mint' evt_type,
            a.collector buyer,
            c.to seller,
            c.handle,
            a.profileId profile_id,
            a.essenceId content_id,
            b.essenceTokenURI content_uri
    from {{source('link3_profile_bnb', 'ProfileNFT_evt_CollectEssence')}} a
    left join {{source('link3_profile_bnb', 'ProfileNFT_evt_RegisterEssence')}} b on a.essenceId = b.essenceId and a.profileId = b.profileId
    left join {{source('link3_profile_bnb', 'ProfileNFT_evt_CreateProfile')}} c on a.profileId = c.profileId
    WHERE 1=1
    {% if is_incremental() %}
    AND a.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND a.evt_block_time >= '{{project_start_date}}'
    {% endif %}
),

cyberconnect_subscribe as (
    select 
            'bnb' as blockchain,
            'cyberconnect' project,
            'collect' action,
            'Subscribe' name,
            a.contract_address,
            a.evt_block_time,
            date_trunc('day', a.evt_block_time) AS block_date,
            a.evt_block_number,
            a.evt_tx_hash,
            a.evt_index,
            'Mint' evt_type,
            a.sender buyer,
            b.to seller,
            b.handle,
            a.profile_id,
            cast(NULL as uint256) AS  content_id,
            cast(NULL as varchar) AS  content_uri
    from  (select *, explode(profileIds) profile_id from {{source('link3_profile_bnb', 'ProfileNFT_evt_Subscribe')}}) a
    left join  {{source('link3_profile_bnb', 'ProfileNFT_evt_CreateProfile')}} b on a.profile_id = b.profileId
    WHERE 1=1
    {% if is_incremental() %}
    AND a.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND a.evt_block_time >= '{{project_start_date}}'
    {% endif %}
),

UNIONED_DATA as 

(
select * from cyberconnect_profile_create
    UNION ALL
select * from cyberconnect_essence_register
    UNION ALL
select * from cyberconnect_essence_collect
    UNION ALL
select * from cyberconnect_subscribe

)
select * from  UNIONED_DATA 
