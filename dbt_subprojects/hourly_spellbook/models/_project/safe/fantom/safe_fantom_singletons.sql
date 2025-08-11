{{ 
    config(
        materialized='table',
        schema = 'safe_fantom',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["fantom"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz"]\') }}'
    ) 
}}

{{ safe_singletons_by_network_validated('fantom', only_official=true) }}