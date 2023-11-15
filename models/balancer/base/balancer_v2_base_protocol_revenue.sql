{% set blockchain = 'base' %}

{{
    config(
        schema='balancer_v2_base',
        alias = 'protocol_revenue', 
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["base"]\',
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
