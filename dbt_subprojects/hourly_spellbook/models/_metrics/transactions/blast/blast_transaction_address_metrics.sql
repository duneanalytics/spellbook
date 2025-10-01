{{ config
    (
        schema = 'blast',
        alias = 'transaction_address_metrics',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_hour', 'from_address', 'to_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_hour')],
        tags=['static'],
        post_hook='{{ hide_spells() }}'
    )
}}

{{
    blockchain_transaction_address_metrics(
        blockchain = 'blast'
    )
}}