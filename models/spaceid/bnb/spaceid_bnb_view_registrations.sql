{{config(alias='view_registrations',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "spaceid",
                                    \'["springzh"]\') }}')}}
SELECT 'v3' as version,
    evt_block_time as block_time,
    name,
    label,
    owner,
    cast(cost as double) as cost,
    cast(expires as bigint) as expires,
    contract_address,
    evt_tx_hash as tx_hash,
    evt_block_number as block_number,
    evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV3_evt_NameRegistered')}}

UNION ALL

SELECT 'v4' as version,
    evt_block_time as block_time,
    name,
    label,
    owner,
    cast(cost as double) as cost,
    cast(expires as bigint) as expires,
    contract_address,
    evt_tx_hash as tx_hash,
    evt_block_number as block_number,
    evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV4_evt_NameRegistered')}}

UNION ALL

SELECT 'v5' as version,
    evt_block_time as block_time,
    name,
    label,
    owner,
    cast(cost as double) as cost,
    cast(expires as bigint) as expires,
    contract_address,
    evt_tx_hash as tx_hash,
    evt_block_number as block_number,
    evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV5_evt_NameRegistered')}}

UNION ALL

SELECT 'v6' as version,
    evt_block_time as block_time,
    name,
    label,
    owner,
    cast(cost as double) as cost,
    cast(expires as bigint) as expires,
    contract_address,
    evt_tx_hash as tx_hash,
    evt_block_number as block_number,
    evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV6_evt_NameRegistered')}}

UNION ALL

SELECT 'v7' as version,
    evt_block_time as block_time,
    name,
    label,
    owner,
    cast(cost as double) as cost,
    cast(expires as bigint) as expires,
    contract_address,
    evt_tx_hash as tx_hash,
    evt_block_number as block_number,
    evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV7_evt_NameRegistered')}}

UNION ALL

SELECT 'v8' as version,
    evt_block_time as block_time,
    name,
    label,
    owner,
    cast(cost as double) as cost,
    cast(expires as bigint) as expires,
    contract_address,
    evt_tx_hash as tx_hash,
    evt_block_number as block_number,
    evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV8_evt_NameRegistered')}}

UNION ALL

SELECT 'v9' as version,
    evt_block_time as block_time,
    name,
    label,
    owner,
    cast(cost as double) as cost,
    cast(expires as bigint) as expires,
    contract_address,
    evt_tx_hash as tx_hash,
    evt_block_number as block_number,
    evt_index
FROM {{source('spaceid_bnb', 'BNBRegistrarControllerV9_evt_NameRegistered')}}
