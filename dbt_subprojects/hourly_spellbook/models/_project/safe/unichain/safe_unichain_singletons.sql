{{ 
    config(
        materialized='table',
        schema = 'safe_unichain',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["unichain"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz", "peterrliem", "safehjc"]\') }}'
    ) 
}}

{{ safe_singletons_by_network('unichain') }}
