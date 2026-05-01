{{
    config(
        schema = 'oneinch',
        alias = 'mapped_contracts',
        materialized = 'table',
        unique_key = ['blockchain', 'address'],
    )
}}



{% for blockchain in oneinch_project_swaps_exposed_blockchains_list() %}
    select * from {{ ref('oneinch_' + blockchain + '_mapped_contracts') }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}