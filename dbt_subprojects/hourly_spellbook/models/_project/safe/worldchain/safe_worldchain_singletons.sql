{{ 
    config(
        materialized='table',
        schema = 'safe_worldchain',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["worldchain"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida"]\') }}'
    ) 
}}

{{ safe_singletons_by_network_validated('worldchain', only_official=true) }}