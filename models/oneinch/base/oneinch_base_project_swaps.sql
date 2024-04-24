{% set blockchain = 'base' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['unique_key']
    )
}}



{{
    oneinch_project_swaps_macro(
        blockchain = blockchain
        , date_from = '2024-01-01'
    )
}}