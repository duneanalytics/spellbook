{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema='balancer_v2_' + blockchain,
        alias = 'protocol_fee', 
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'pool_id'],
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
