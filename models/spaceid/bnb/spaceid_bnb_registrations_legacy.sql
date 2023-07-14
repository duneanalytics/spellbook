{{
    config(
	tags=['legacy'],
	
        alias = alias('registrations', legacy_model=True)
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,unique_key = ['name']
        ,post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "spaceid",
                                    \'["springzh"]\') }}'
    )
}}
SELECT 'v3'                    as version,
       evt_block_time          as block_time,
       name,
       label,
       owner,
       cast(cost as double)    as cost,
       cast(expires as bigint) as expires,
       contract_address,
       evt_tx_hash             as tx_hash,
       evt_block_number        as block_number,
       evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV3_evt_NameRegistered')}}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION ALL

SELECT 'v4'                    as version,
       evt_block_time          as block_time,
       name,
       label,
       owner,
       cast(cost as double)    as cost,
       cast(expires as bigint) as expires,
       contract_address,
       evt_tx_hash             as tx_hash,
       evt_block_number        as block_number,
       evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV4_evt_NameRegistered')}}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION ALL

SELECT 'v5'                    as version,
       evt_block_time          as block_time,
       name,
       label,
       owner,
       cast(cost as double)    as cost,
       cast(expires as bigint) as expires,
       contract_address,
       evt_tx_hash             as tx_hash,
       evt_block_number        as block_number,
       evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV5_evt_NameRegistered')}}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION ALL

SELECT 'v6'                    as version,
       evt_block_time          as block_time,
       name,
       label,
       owner,
       cast(cost as double)    as cost,
       cast(expires as bigint) as expires,
       contract_address,
       evt_tx_hash             as tx_hash,
       evt_block_number        as block_number,
       evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV6_evt_NameRegistered')}}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION ALL

SELECT 'v7'                    as version,
       evt_block_time          as block_time,
       name,
       label,
       owner,
       cast(cost as double)    as cost,
       cast(expires as bigint) as expires,
       contract_address,
       evt_tx_hash             as tx_hash,
       evt_block_number        as block_number,
       evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV7_evt_NameRegistered')}}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION ALL

SELECT 'v8'                    as version,
       evt_block_time          as block_time,
       name,
       label,
       owner,
       cast(cost as double)    as cost,
       cast(expires as bigint) as expires,
       contract_address,
       evt_tx_hash             as tx_hash,
       evt_block_number        as block_number,
       evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV8_evt_NameRegistered')}}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION ALL

-- There are some records in v9 table are duplicated with those in v5 table. So we join to exclude them
SELECT 'v9'                       as version,
       v9.evt_block_time          as block_time,
       v9.name,
       v9.label,
       v9.owner,
       cast(v9.cost as double)    as cost,
       cast(v9.expires as bigint) as expires,
       v9.contract_address,
       v9.evt_tx_hash             as tx_hash,
       v9.evt_block_number        as block_number,
       v9.evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV9_evt_NameRegistered')}} v9
LEFT JOIN {{source('spaceid_bnb', 'BNBRegistrarControllerV5_evt_NameRegistered')}} v5
    ON v9.name = v5.name
WHERE v5.name is null
  {% if is_incremental() %}
  AND v9.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}
