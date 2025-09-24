{{  
    config(
        schema = 'oneinch_evms',
        alias = 'ptfc',
        materialized = 'view'
    )
}}

{% set meta = oneinch_meta_cfg_macro()['blockchains'] %}

-- View for all parsed transfers from calls, for internal usage. Won't be used in lineage --

{% for blockchain, category in meta['category'].items() if category == 'evms' and blockchain in meta['exposed'] %}
    select * from ({{ oneinch_ptfc_macro(blockchain = blockchain) }})
    {% if not loop.last %}union all{% endif %}
{% endfor %}
