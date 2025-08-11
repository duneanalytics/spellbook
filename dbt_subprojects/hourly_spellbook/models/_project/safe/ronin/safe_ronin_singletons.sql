{{ 
    config(
        materialized='table',
        schema = 'safe_ronin',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["ronin"]\',
                                    "project",
                                    "safe",
                                    \'["petertherock"]\') }}'
    ) 
}}

{{ safe_singletons_by_network('ronin') }}
