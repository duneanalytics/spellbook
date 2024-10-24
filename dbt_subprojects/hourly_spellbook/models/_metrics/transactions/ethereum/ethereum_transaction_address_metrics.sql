{{ config
    (
        schema = 'ethereum',
        alias = 'transaction_address_metrics',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_hour', 'from_address', 'to_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_hour')],
        post_hook = '{{ expose_spells(\'["ethereum"]\',
                                        "sector",
                                        "metrics",
                                        \'["jeff-dude"]\') }}'
    )
}}

{{
    blockchain_transaction_address_metrics(
        blockchain = 'ethereum'
    )
}}