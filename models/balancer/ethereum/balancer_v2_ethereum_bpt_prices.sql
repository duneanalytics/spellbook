{% set blockchain = 'ethereum' %}

{{
    config(
        schema = 'balancer_v2_ethereum',
        alias = 'bpt_prices',        
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'blockchain', 'contract_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
    )
}}


{{ 
    balancer_bpt_prices_macro(
        blockchain = blockchain,
        version = '2'
    )
}}
