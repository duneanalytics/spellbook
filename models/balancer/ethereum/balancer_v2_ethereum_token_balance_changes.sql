{% set blockchain = 'ethereum' %}

{{
    config(
        schema = 'balancer_v2_ethereum',
        alias = 'token_balance_changes', 
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'evt_tx_hash', 'evt_index', 'token_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')]
    )
}}

{{ 
    balancer_token_balance_changes_macro(
        blockchain = blockchain,
        version = '2'
    )
}}
