{%- set blockchain = 'fantom' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps_second_side',
        partition_by = ['block_month', 'project'],
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
    )
-}}

{{-
    oneinch_project_swaps_second_side_macro(
        blockchain = blockchain,
        project_swaps_base_table = ref('oneinch_' ~ blockchain ~ '_project_swaps_base')
    )
-}}
