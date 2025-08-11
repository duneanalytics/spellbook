{{ 
    config(
        materialized='table',
        schema = 'safe_linea',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["linea"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida"]\') }}'
    ) 
}}

{{ safe_singletons_by_network('linea') }}
