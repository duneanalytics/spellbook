{{
    config(
        materialized='table',
        schema = 'safe_ethereum',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz", "danielpartida"]\') }}'
    )
}}

{{ safe_singletons_by_network_validated('ethereum', only_official=true) }}