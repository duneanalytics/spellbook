{{ config
    (
        schema = 'blast',
        alias = 'transaction_address_metrics',
        materialized = 'table',
        file_format = 'delta',
        tags=['static'],
        post_hook='{{ hide_spells() }}'
    )
}}

{{
    blockchain_transaction_address_metrics(
        blockchain = 'blast'
    )
}}