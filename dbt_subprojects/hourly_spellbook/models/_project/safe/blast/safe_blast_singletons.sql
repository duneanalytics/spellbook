{{ 
    config(
        materialized='table',
        schema = 'gnosis_safe_blast',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["blast"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida"]\') }}'
    ) 
}}

{{ safe_singletons_by_network('blast') }}
