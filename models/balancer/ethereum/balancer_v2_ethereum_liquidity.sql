
{% set blockchain = 'ethereum' %}

{{
    config(
        schema = 'balancer_v2_ethereum',
        alias = 'liquidity',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'blockchain', 'pool_id', 'token_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    )
}}


{{ 
    balancer_liquidity_macro(
        blockchain = blockchain,
        version = '2'
    )
}}