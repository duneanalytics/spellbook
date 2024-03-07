{% set blockchain = 'arbitrum' %}

{{
    config(
        schema = 'balancer_v2_arbitrum',
        alias = 'protocol_fee', 
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'pool_id', 'token_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]

    )
}}

{{ 
    balancer_protocol_fee_macro(
        blockchain = blockchain,
        version = '2'
    )
}}
