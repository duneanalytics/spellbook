{{ config(
    alias = 'resolver_addresses',
    materialized = 'incremental',
    unique_key = ['address']
    )
}}

WITH reverse_records AS (
SELECT b.*, t.from AS address FROM
(
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY name ORDER BY call_block_time DESC) AS latest_ens
    FROM
    ( 
        SELECT _name as name, call_tx_hash, call_block_time
        FROM {{source('ethereumnameservice_ethereum', 'DefaultReverseResolver_call_setName')}} rs
        WHERE call_success

        UNION ALL
        SELECT name, call_tx_hash, call_block_time
        FROM {{source('ethereumnameservice_ethereum', 'PublicResolver_call_setName')}} rs
        WHERE call_success

        UNION ALL
        SELECT name, call_tx_hash, call_block_time
        FROM {{source('ethereumnameservice_ethereum', 'ReverseRegistrar_v2_call_setName')}} vs
        WHERE call_success

        UNION ALL
        SELECT name, call_tx_hash, call_block_time
        FROM {{source('ethereumnameservice_ethereum', 'ReverseRegistrar_v1_call_setName')}} vs
        WHERE call_success

    ) a
) b
INNER JOIN {{ source('ethereum','transactions') }} t
ON b.call_tx_hash = t.hash
AND b.call_block_time = t.block_time
WHERE latest_ens = 1
)

-- for when the resolve wasn't as simple?
, register_to_resolve AS (
    WITH regs AS (
    SELECT r.*, COALESCE(nr.id,bnr.id) AS id
    FROM (
        SELECT name || '.eth' AS name, evt_tx_hash, evt_block_time, owner
            FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_1_evt_NameRegistered')}}
        UNION ALL
        SELECT name || '.eth' AS name, evt_tx_hash, evt_block_time, owner
            FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_2_evt_NameRegistered')}}
        UNION ALL
        SELECT name || '.eth' AS name, evt_tx_hash, evt_block_time, owner
            FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_3_evt_NameRegistered')}}
        ) r
    LEFT JOIN {{source('ethereumnameservice_ethereum', 'OldBaseRegistrar_evt_NameRegistered')}} nr
        ON nr.evt_tx_hash = r.evt_tx_hash
    LEFT JOIN {{source('ethereumnameservice_ethereum', 'BaseRegistrarImplementation_evt_NameRegistered')}} bnr
        ON bnr.evt_tx_hash = r.evt_tx_hash
    )
    
    ,reclaims AS (
        SELECT r.name, call_tx_hash, call_block_time, rcc.owner, r.id 
            FROM {{source('ethereumnameservice_ethereum', 'BaseRegistrarImplementation_call_reclaim')}} rcc
        INNER JOIN regs r
            ON r.id = rcc.id
            AND rcc.call_block_time >= r.evt_block_time
        )
    ,get_nodes AS (
    SELECT DISTINCT *
    FROM (
        SELECT r.name, r.evt_tx_hash, r. evt_block_time, ac.node 
            FROM {{source('ethereumnameservice_ethereum', 'PublicResolver_evt_AddrChanged')}} ac
            INNER JOIN regs r
                ON r.evt_tx_hash = ac.evt_tx_hash
                
        UNION ALL
        
        SELECT r.name, r.evt_tx_hash, r. evt_block_time, aac.node 
            FROM {{source('ethereumnameservice_ethereum', 'PublicResolver_evt_AddressChanged')}} aac
            INNER JOIN regs r
                ON r.evt_tx_hash = aac.evt_tx_hash
        ) r
    )
    
    ,address_changes AS ( --similar to getting nodes, but using those nodes to pull the latest address changes
    SELECT DISTINCT *
    FROM (
        SELECT n.name, ac.a AS owner, ac.evt_tx_hash, ac. evt_block_time, ac.node
        FROM {{source('ethereumnameservice_ethereum', 'PublicResolver_evt_AddrChanged')}} ac
            INNER JOIN get_nodes n
                ON n.node = ac.node
                
        UNION ALL
        
        SELECT n.name, aac.newAddress AS owner, aac.evt_tx_hash, aac. evt_block_time, aac.node 
        FROM {{source('ethereumnameservice_ethereum', 'PublicResolver_evt_AddressChanged')}} aac
            INNER JOIN get_nodes n
                ON n.node = aac.node
            WHERE coinType ='60' --Only care about eth, but we could change this if we wanted: https://github.com/satoshilabs/slips/blob/master/slip-0044.md
        ) chg
    )
        
    SELECT *, ROW_NUMBER() OVER (PARTITION BY name ORDER BY block_time DESC)
    FROM (
        SELECT name, evt_tx_hash AS tx_hash, evt_block_time AS block_time, owner FROM regs
        UNION ALL
        SELECT name, call_tx_hash AS tx_hash, call_block_time AS block_time, owner FROM reclaims
        UNION ALL
        SELECT name, evt_tx_hash AS tx_hash, evt_block_time AS block_time, owner FROM address_changes
        ) names
)

SELECT address
, name as ens_name
, tx_hash as latest_tx
, block_time as latest_tx_block_time
FROM
    (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY name ORDER BY block_time DESC) AS name_rank
    FROM (
        SELECT name, tx_hash, block_time, owner AS address FROM register_to_resolve
        UNION ALL
        SELECT name, call_tx_hash AS tx_hash, call_block_time AS block_time, address FROM reverse_records
        ) lastone
    ) fin
WHERE name_rank = 1