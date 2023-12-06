
{% set blockchain = 'polygon' %}

{{
    config(
        schema='balancer_v2_' + blockchain,
        alias = 'liquidity',
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["{{blockchain}}"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon", "viniabussafi"]\') }}'
    )
}}

{{ 
    balancer_protocol_revenue_macro(
        blockchain = blockchain,
    )
}}
