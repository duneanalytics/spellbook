{% macro oneinch_exposed_blockchains_list() %}
   {{ return([
        'ethereum', 
        'optimism', 
        'arbitrum', 
        'avalanche_c', 
        'polygon', 
        'bnb', 
        'gnosis', 
        'fantom', 
        'base', 
    ]) }}
{% endmacro %}