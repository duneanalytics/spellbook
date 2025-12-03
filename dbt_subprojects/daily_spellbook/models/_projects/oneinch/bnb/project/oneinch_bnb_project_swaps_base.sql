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
        '2020_0812',
        '2021_0106',
        '2021_0709',
        '2021_1012',
        '2022_0104',
        '2022_0512',
        '2023_0112',
        '2024_0112',
        '2025_0103',
        '2025_0406',
        '2025_0707',
        '2025_0808',
        '2025_0909',
        'current',
    ]
-%}

{%- for part in parts %}
    select * from {{ ref('oneinch_' + blockchain + '_project_swaps_base_' + part) }}
    {% if not loop.last -%} union all {%- endif %}
{%- endfor -%}