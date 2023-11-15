{% set blockchain = 'ethereum' %}

{{
    config(
        schema='balancer_v2_ethereum',
        alias = 'protocol_revenue', 
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["viniabussafi"]\') }}'
    )
}}

{{ 
    balancer_protocol_fees_macro(
        blockchain = blockchain,
    )
}}
