{%- set blockchain = 'bnb' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps_base',
        materialized = 'view',
        unique_key = ['blockchain', 'id'],
    )
-}}

{%-
    set parts = [
        '2020',
        '2021_01',
        '2021_02',
        '2022',
        '2023',
        '2024',
        '2025_01',
        'current',
    ]
-%}

{%- for part in parts %}
    select * from {{ ref('oneinch_' + blockchain + '_project_swaps_base_' + part) }}
    {% if not loop.last -%} union all {%- endif %}
{%- endfor -%}