{{ 
    config(
        materialized='table',
        schema = 'safe_polygon',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz"]\') }}'
    ) 
}}

{{ safe_singletons_by_network('polygon') }}
