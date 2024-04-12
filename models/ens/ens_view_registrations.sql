{{config(
        alias = 'view_registrations',
        
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "ens",
                                    \'["antonio-mendes","mewwts"]\') }}'
)}}


SELECT *
FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_1_evt_NameRegistered')}}
UNION 
SELECT *
FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_2_evt_NameRegistered')}}
UNION 
SELECT *
FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_3_evt_NameRegistered')}}
UNION 
SELECT 
        contract_address
        ,evt_tx_hash
        ,evt_index
        ,evt_block_time
        ,evt_block_number
        ,baseCost
        ,expires
        ,label
        ,name
        ,owner
FROM {{source('ethereumnameservice_ethereum','ETHRegistrarController_4_evt_NameRegistered')}}
