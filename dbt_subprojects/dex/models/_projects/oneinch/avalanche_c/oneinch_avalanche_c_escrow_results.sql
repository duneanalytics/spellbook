{% set blockchain = 'avalanche_c' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'escrow_results',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'tx_hash', 'trace_address']
    )
}}



{{ oneinch_escrow_results_macro(blockchain) }}