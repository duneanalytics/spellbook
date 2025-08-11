{{ 
    config(
        materialized='table',
        schema = 'safe_zksync',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["zksync"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida", "kryptaki"]\') }}'
    ) 
}}

{{ safe_singletons_by_network('zksync') }}