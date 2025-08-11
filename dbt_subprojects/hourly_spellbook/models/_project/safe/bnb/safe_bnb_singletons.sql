{{
    config(
        materialized='table',
        schema = 'safe_bnb',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz"]\') }}'
    )
}}

{{ safe_singletons_by_network('bnb') }}