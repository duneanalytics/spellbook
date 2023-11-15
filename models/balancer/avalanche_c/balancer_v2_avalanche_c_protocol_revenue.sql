{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema='balancer_v2_avalanche_c',
        alias = 'protocol_revenue', 
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
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
