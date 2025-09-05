{% set blockchain = 'bnb' %}

{{
    config(
        schema = 'tokens_' ~ blockchain,
        alias = 'base_transfers_traces',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_date','unique_key'],
    )
}}



{{ transfers_traces_base(blockchain=blockchain, easy_dates=True) }}
