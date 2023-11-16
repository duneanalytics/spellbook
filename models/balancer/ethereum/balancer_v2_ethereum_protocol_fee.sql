{% set blockchain = 'ethereum' %}

{{
    config(
        schema='balancer_v2_' + blockchain,
        alias = 'protocol_fee', 
        materialized = 'incremental',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
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
