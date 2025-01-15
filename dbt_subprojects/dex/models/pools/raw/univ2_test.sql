{{ config(
        schema = 'uni_v2',
        alias = 'uni_v2',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{{univ2_macro(
        blockchain = 'polygon',
        logs = source('polygon', 'logs')
)}}
