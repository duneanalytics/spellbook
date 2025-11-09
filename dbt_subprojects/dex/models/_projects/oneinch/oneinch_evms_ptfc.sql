{{  
    config(
        schema = 'oneinch_evms',
        alias = 'ptfc',
        materialized = 'view',
    )
}}

-- View for all parsed transfers from calls, for internal usage. Won't be used in lineage --

{% for blockchain in oneinch_blockchains_cfg_macro() if blockchain.exposed and blockchain.evm %}
    select * from ({{ oneinch_ptfc_macro(blockchain = blockchain.name) }})
    {% if not loop.last -%} union all {%- endif %}
{% endfor %}