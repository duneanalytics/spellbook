{% set blockchain = 'bnb' %}

{{
    config(
        schema = 'tokens_' ~ blockchain,
        alias = 'base_transfers_traces_wrapped_token_deposit',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_date','unique_key'],
    )
}}



{{ transfers_traces_wrapped_token_deposit_base(blockchain=blockchain, transfers_traces_base_table=ref('tokens_' ~ blockchain ~ '_base_transfers_traces')) }}