{{
    config(
        schema = 'oneinch',
        alias = 'blockchains',
        materialized = 'table',
        unique_key = ['blockchain'],
    )
}}



{% for blockchain in oneinch_exposed_blockchains_list() %}
    {{ oneinch_blockchain_macro(blockchain) }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}
