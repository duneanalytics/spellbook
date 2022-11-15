{{ config(
        alias ='swaps',
        post_hook='{{ expose_spells(\'["avalanche_c","arbitrum"]\',
                                "project",
                                "gmx",
                                \'["henrystats"]\') }}'
        )
}}

SELECT * FROM {{ ref('gmx_avalanche_c_swaps_trades') }}

UNION 

SELECT * FROM {{ ref('gmx_arbitrum_swaps_trades') }}