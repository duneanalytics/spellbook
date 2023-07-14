{{config(
	tags=['legacy'],
	alias = alias('view_registrations', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "ens",
                                    \'["antonio-mendes","mewwts"]\') }}')}}
SELECT *
FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_1_evt_NameRegistered')}}
UNION 
SELECT *
FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_2_evt_NameRegistered')}}
UNION 
SELECT *
FROM {{source('ethereumnameservice_ethereum', 'ETHRegistrarController_3_evt_NameRegistered')}}
