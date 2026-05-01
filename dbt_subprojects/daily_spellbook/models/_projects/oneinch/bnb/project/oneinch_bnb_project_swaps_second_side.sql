{%- set blockchain = 'bnb' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps_second_side',
        partition_by = ['block_month', 'project'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'id'],
    )
-}}

{{-
    oneinch_project_swaps_second_side_macro(
        blockchain = blockchain,
        project_swaps_base_table = ref('oneinch_' ~ blockchain ~ '_project_swaps_base')
    )
-}}
