{{
    config(
        schema = 'oneinch',
        alias = 'mapped_methods',
        materialized = 'table',
        unique_key = ['blockchain', 'address', 'signature'],
    )
}}



{% for blockchain in oneinch_project_swaps_exposed_blockchains_list() %}
    select * from {{ ref('oneinch_' + blockchain + '_mapped_methods') }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}