{%- set blockchain = 'bnb' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps_base',
        materialized = 'view',
        unique_key = ['block_month', 'id'],
    )
-}}

{%-
    set parts = [
        '2020_0712',
        '2021_0104',
        '2021_0505',
        '2021_0607',
        '2021_0809',
        '2021_1010',
        '2021_1111',
        '2021_1212',
        '2022_0104',
        '2022_0512',
        '2023_0112',
        '2024_0106',
        '2024_0712',
        '2025_0103',
        '2025_0405',
        '2025_0606',
        '2025_0707',
        '2025_0808',
        '2025_0909',
        '2025_1010',
        'current',
    ]
-%}

{%- for part in parts %}
    select * from {{ ref('oneinch_' + blockchain + '_project_swaps_base_' + part) }}
    {% if not loop.last -%} union all {%- endif %}
{%- endfor -%}