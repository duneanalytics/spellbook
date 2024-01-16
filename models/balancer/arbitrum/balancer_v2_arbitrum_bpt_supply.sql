{% set blockchain = 'arbitrum' %}

{{ config(
    schema = 'balancer_v2_arbitrum',
    materialized = 'table',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'pool_id', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
    alias = 'bpt_supply',
    )
}}

{{ 
    balancer_bpt_supply_macro(
        blockchain = blockchain,
        version = '2'
    )
}}