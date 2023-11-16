{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema='balancer_v2_' + blockchain,
        alias = 'protocol_fee', 
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["viniabussafi"]\') }}'
    )
}}

{{ 
    balancer_protocol_fee_macro(
        blockchain = blockchain,
    )
}}
