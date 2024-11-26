{{ config(
    schema = 'lifi_ethereum',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with source_data as (
    {{ lifi_extract_bridge_data('ethereum') }}
)

{{
    add_tx_columns(
        model_cte = 'source_data'
        , blockchain = 'ethereum'
        , columns = ['from']
    )
}}