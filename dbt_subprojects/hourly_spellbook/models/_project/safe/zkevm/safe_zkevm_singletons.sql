{{ 
    config(
        materialized='table',
        schema = 'safe_zkevm',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["zkevm"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida"]\') }}'
    ) 
}}

{{ safe_singletons_by_network_validated('zkevm', only_official=true) }}