{{
    config(
        schema = 'oneinch',
        alias = 'mapped_contracts',
        materialized = 'table',
        on_table_exists = 'drop',
        tags = ['prod_exclude'],
        unique_key = ['blockchain', 'address'],
    )
}}



{% for blockchain in oneinch_exposed_blockchains_list() %}
    select * from ({{ oneinch_mapped_contracts_macro(blockchain) }})
    {% if not loop.last %} union all {% endif %}
{% endfor %}