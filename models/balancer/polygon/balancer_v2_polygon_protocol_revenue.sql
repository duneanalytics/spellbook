{% set blockchain = 'polygon' %}

{{
    config(
        schema='balancer_v2_polygon',
        alias = 'protocol_revenue', 
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["viniabussafi"]\') }}'
    )
}}

{{ 
    balancer_protocol_revenue_macro(
        blockchain = blockchain,
    )
}}
