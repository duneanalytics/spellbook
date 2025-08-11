{{ 
    config(
        materialized='table',
        schema = 'safe_scroll',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["scroll"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida"]\') }}'
    ) 
}}

{{ safe_singletons_by_network('scroll') }}