{{  
    config(
        schema = 'oneinch_evms',
        alias = 'ptfc',
        materialized = 'view'
    )
}}

-- View for all parsed transfers from calls, for internal usage. Won't be used in lineage --

{% for blockchain, exposed in oneinch_meta_cfg_macro()['blockchains']['exposed'].items() if exposed == 'evms' %}
    select * from ({{ oneinch_ptfc_macro(blockchain) }})
    {% if not loop.last %}union all{% endif %}
{% endfor %}
