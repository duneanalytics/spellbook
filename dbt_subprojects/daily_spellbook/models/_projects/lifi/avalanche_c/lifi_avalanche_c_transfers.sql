{{ config(
    schema = 'lifi_avalanche_c',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with source_data as (
    {{ lifi_extract_bridge_data('avalanche_c') }}
)

{{
    add_tx_columns(
        model_cte = 'source_data'
        , blockchain = 'avalanche_c'
        , columns = ['from']
    )
}}
