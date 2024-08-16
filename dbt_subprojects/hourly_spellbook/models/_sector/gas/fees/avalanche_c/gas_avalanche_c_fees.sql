{% set blockchain = 'avalanche_c' %}

{{ config(
    schema = 'gas_' + blockchain
    ,alias = 'fees'
    ,partition_by = ['block_month']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy='merge'
    ,unique_key = ['block_month', 'block_number', 'tx_hash']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    gas_fees_london
    (
        blockchain = blockchain
        , token_symbol = 'AVAX'
    )
}}