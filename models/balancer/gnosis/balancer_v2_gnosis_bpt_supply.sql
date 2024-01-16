{% set blockchain = 'gnosis' %}

{{
    config(
        schema='balancer_v2_' + blockchain,
        alias = 'protocol_fee', 
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'token_address', 'blockchain'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]

    )
}}

{{ 
    balancer_protocol_fee_macro(
        blockchain = blockchain,
        version = '2'
    )
}}