{{ 
    config(
        materialized='table',
        schema = 'safe_optimism',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz", "peterrliem"]\') }}'
    ) 
}}

{{ safe_singletons_by_network('optimism') }}