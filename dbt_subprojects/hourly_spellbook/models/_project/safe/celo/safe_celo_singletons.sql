{{ 
    config(
        materialized='table',
        schema = 'safe_celo',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida"]\') }}'
    ) 
}}

{{ safe_singletons_by_network('celo') }}