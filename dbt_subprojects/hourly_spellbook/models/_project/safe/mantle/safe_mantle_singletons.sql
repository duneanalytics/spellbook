{{ 
    config(
        materialized='table',
        schema = 'safe_mantle',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["mantle"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida"]\') }}'
    ) 
}}

{{ safe_singletons_by_network('mantle') }}