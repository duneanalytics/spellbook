{{config(alias='view_renewals')}}
SELECT *
FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_1_evt_NameRenewed')}}
UNION 
SELECT *
FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_2_evt_NameRenewed')}}
UNION 
SELECT *
FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_3_evt_NameRenewed')}} ;
