{{ 
    config(
        materialized='table',
        schema = 'safe_berachain',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["berachain"]\',
                                    "project",
                                    "safe",
                                    \'["petertherock"]\') }}'
    ) 
}}

{{ safe_singletons_by_network('berachain') }}
