{{config(alias='view_registrations')}}
SELECT *
FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_1_evt_NameRegistered')}}
UNION 
SELECT *
FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_2_evt_NameRegistered')}}
UNION 
SELECT *
FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_3_evt_NameRegistered')}}
