{{
    config(
        materialized='table',
        schema = 'safe_base',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["base"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida", "peterrliem"]\') }}'
    )
}}

{{ safe_singletons_by_network_validated('base', only_official=true) }}
