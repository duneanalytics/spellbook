{{ config(
    schema = 'curve_monad',
    alias = 'view_pools',
    materialized = 'table',
    file_format = 'delta'
    , post_hook='{{ hide_spells() }}'
    )
}}

{# Pool registry for curve on monad. Only Factory V1 Plain pools exist so far —
   the StableSwapFactory (0xFF5Cb29241F002fFeD2eAa224e3e996D24A6E8d1) is the
   sole pool deployer on Monad. Mirrors curve_ethereum_view_pools but with the
   single-factory subset that applies here. #}

SELECT
    'Factory V1 Plain'              AS version,
    pool                            AS pool_address,
    coins                           AS coins,
    CAST(NULL AS array(varbinary))  AS undercoins
FROM {{ source('curvefi_monad', 'stableswapfactory_evt_plainpooldeployed') }}
