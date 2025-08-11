{{
    config(
        materialized='table',
        schema = 'safe_avalanche_c',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz"]\') }}'
    )
}}

{{ safe_singletons_by_network('avalanche_c') }}