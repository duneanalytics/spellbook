{{  
    config(
        schema = 'oneinch_evms',
        alias = 'ptfc',
        materialized = 'view'
    )
}}

-- View for all parsed transfers from calls, for internal usage. Won't be used in lineage --

{% for blockchain in oneinch_meta_cfg_macro()['blockchains']['evms'] %}
    select * from ({{ oneinch_ptfc_macro(blockchain = blockchain) }})
    {% if not loop.last %}union all{% endif %}
{% endfor %}
