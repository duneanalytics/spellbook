{{  
    config(
        schema = 'oneinch',
        alias = 'parsed_transfers_from_calls',
        materialized = 'view'
    )
}}

-- View for all parsed transfers from calls, for internal usage. Won't be used in lineage.

{% for blockchain in oneinch_exposed_blockchains_list() %}
    select * from ({{ oneinch_parsed_transfers_from_calls_macro(blockchain) }})
    {% if not loop.last %}union all{% endif %}
{% endfor %}
