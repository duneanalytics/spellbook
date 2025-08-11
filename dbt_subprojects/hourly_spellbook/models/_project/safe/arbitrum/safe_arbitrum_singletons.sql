{{ 
    config(
        materialized='table',
        schema = 'safe_arbitrum',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz", "peterrliem"]\') }}'
    ) 
}}

{{ safe_singletons_by_network_validated('arbitrum', only_official=true) }}
