{% set blockchain = 'zkevm' %}

{{
    config(
        schema = 'balancer_v2_zkevm',
        alias = 'bpt_supply',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'blockchain', 'token_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
    )
}}

{{ 
    balancer_bpt_supply_macro(
        blockchain = blockchain,
        version = '2'
    )
}}